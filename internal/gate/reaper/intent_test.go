package reaper

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// crashPoint arms a writer to crash at one point of a conditional write.
type crashPoint struct {
	name string
	arm  func(g *intentGate)
	// committed is whether the write's transaction applied before the crash.
	committed bool
}

var crashPoints = []crashPoint{
	{name: "after the intent", arm: func(g *intentGate) { g.wm.crashAfterIntent = true }},
	{name: "after the prepare", arm: func(g *intentGate) {
		g.wm.setHook(func(int, txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
			g.wm.dead.Store(true)
			return meta.TxnUnknown, nil, errCrashed
		})
	}},
	{name: "after the transaction", committed: true, arm: crashAfterTheTransaction},
	{name: "after the shard commit", committed: true, arm: func(g *intentGate) { g.wm.crashOnIntentDelete = true }},
}

func crashAfterTheTransaction(g *intentGate) {
	g.wm.setHook(func(_ int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		outcome, _, _ := next()
		require.Equal(g.t, meta.TxnApplied, outcome)
		g.wm.dead.Store(true)
		return meta.TxnUnknown, nil, errCrashed
	})
}

// requireConverged holds a settled write to one of its two outcomes: the new
// object readable and live, or the previous object current with nothing of
// the write left on any candidate.
func (g *intentGate) requireConverged(id handlers.IntentID, in handlers.Intent, committed bool) {
	g.t.Helper()
	require.Empty(g.t, g.intents(), "the intent was not settled")
	if committed {
		// Live before any read, so a read's roll-forward cannot stand in for the reaper.
		g.requireLiveAt(id, in)
		g.requireBody("lock", body2)
		assert.Zero(g.t, g.rb.count("abort", id.Epoch), "the reaper aborted the shards of a committed write")
		return
	}
	g.requireBody("lock", body1)
	g.requireNothingPrepared(id, in)
	assert.Zero(g.t, g.rb.count("commit", id.Epoch), "the reaper committed the shards of an aborted write")
}

// A crash at each point of a conditional write leaves an intent, and one
// reaper pass reaches one of two states: the new object readable at the
// floor, or the previous object current. A handoff holder is settled too.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_ACrashAtAnyPointConvergesThroughTheIntent(t *testing.T) {
	for _, c := range crashPoints {
		for _, handoff := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/handoff=%t", c.name, handoff), func(t *testing.T) {
				g := newIntentGate(t)
				etag := g.mustPut("lock", body1)
				if handoff {
					g.wb.refuseHeld = map[config.NodeID]bool{g.owners("lock")[1]: true}
				}
				c.arm(g)

				rr := g.condPut("lock", body2, ifMatch(etag))
				require.True(t, g.wm.dead.Load(), "the writer never reached its crash point")
				if !c.committed {
					require.NotEqual(t, http.StatusOK, rr.Code)
				}
				id, in, _ := g.onlyIntent()
				if handoff {
					require.NotZero(t, in.Handoff)
				}
				if in.State == handlers.IntentPending {
					g.age()
				}

				g.pass()

				g.requireConverged(id, in, c.committed)
			})
		}
	}
}

// Every intent state converges: a pending intent once it is aged, and a
// committed or an aborted one at once, with no wait for the age.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_EveryIntentStateConverges(t *testing.T) {
	for _, c := range []struct {
		name      string
		state     handlers.IntentState
		committed bool
		arm       func(g *intentGate)
	}{
		{name: "pending after a refused transaction", state: handlers.IntentPending, arm: func(g *intentGate) {
			g.wb.abortFails = map[config.NodeID]bool{g.owners("lock")[0]: true}
			g.wm.setHook(func(int, txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
				return meta.TxnRefused, nil, meta.ErrTxnRefused
			})
		}},
		{name: "aborted after a fence the writer won", state: handlers.IntentAborted, arm: func(g *intentGate) {
			g.wb.abortFails = map[config.NodeID]bool{g.owners("lock")[0]: true}
			g.wm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
				if n == 1 {
					return meta.TxnUnknown, nil, meta.ErrTxnUnknown
				}
				return next()
			})
		}},
		{name: "committed after an unanswered commit", state: handlers.IntentCommitted, committed: true, arm: func(g *intentGate) {
			g.wb.commitFails = map[config.NodeID]bool{g.owners("lock")[0]: true}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newIntentGate(t)
			etag := g.mustPut("lock", body1)
			c.arm(g)

			rr := g.condPut("lock", body2, ifMatch(etag))
			require.Equal(t, c.committed, rr.Code == http.StatusOK, "body: %s", rr.Body.String())
			id, in, _ := g.onlyIntent()
			require.Equal(t, c.state, in.State)
			g.wb.abortFails, g.wb.commitFails = nil, nil
			if c.state == handlers.IntentPending {
				g.age()
			}

			g.pass()

			g.requireConverged(id, in, c.committed)
			assert.Equal(t, int64(1), g.svc.Stats().IntentsSettled)
		})
	}
}

// A pending intent younger than the age may belong to a live writer, so a
// pass sends it nothing. Once the clock passes the age, a pass settles it.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_APendingIntentYoungerThanTheAgeIsUntouched(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	crashPoints[1].arm(g)
	g.condPut("lock", body2, ifMatch(etag))
	id, in, row := g.onlyIntent()
	require.Equal(t, handlers.IntentPending, in.State)

	g.clock.set(in.Created.Add(testIntentAge - time.Millisecond))
	g.pass()

	assert.Equal(t, row, g.intents()[id.Key()], "a young intent was changed")
	assert.Zero(t, g.rm.txnCount(), "a young intent was fenced")
	assert.Zero(t, g.rb.total(), "a young intent's shards were touched")
	stats := g.svc.Stats()
	assert.Equal(t, int64(1), stats.IntentsOwed)
	assert.Zero(t, stats.IntentsFailed)

	g.clock.set(in.Created.Add(testIntentAge))
	g.pass()
	g.requireConverged(id, in, false)
}

// A candidate that does not answer keeps the intent, whatever its state, and
// the acknowledged or previous object stays readable. A pass after the node
// returns settles it.
// Verifies: ADR-0003:S7.
func TestS7_AnUnreachableNodeKeepsTheIntent(t *testing.T) {
	for _, c := range []crashPoint{crashPoints[1], crashPoints[2]} {
		t.Run(c.name, func(t *testing.T) {
			g := newIntentGate(t)
			etag := g.mustPut("lock", body1)
			c.arm(g)
			g.condPut("lock", body2, ifMatch(etag))
			id, in, _ := g.onlyIntent()
			g.age()
			down := in.Nodes[0]
			g.rb.setDown(down, true)

			g.pass()

			require.Len(t, g.intents(), 1, "the intent was dropped while a candidate had not answered")
			_, kept, _ := g.onlyIntent()
			if c.committed {
				assert.Equal(t, handlers.IntentCommitted, kept.State)
				g.requireBody("lock", body2)
				assert.Zero(t, g.rb.count("abort", id.Epoch))
			} else {
				assert.Equal(t, handlers.IntentAborted, kept.State, "an aged pending intent must be fenced")
				g.requireBody("lock", body1)
				assert.Zero(t, g.rb.count("commit", id.Epoch))
			}
			stats := g.svc.Stats()
			assert.Equal(t, int64(1), stats.IntentsFailed)
			assert.Equal(t, int64(1), stats.IntentsOwed)

			g.rb.setDown(down, false)
			g.pass()
			g.requireConverged(id, in, c.committed)
			assert.Zero(t, g.svc.Stats().IntentsOwed)
		})
	}
}

// A lagging follower can answer the scan with the pending copy of an intent
// the leader holds committed. Only the fence decides a pending row, so the
// acknowledged write is committed and never aborted.
// Verifies: ADR-0003:S7.
func TestS7_ALaggingFollowerCannotMakeACommittedIntentLookPending(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	crashAfterTheTransaction(g)
	g.condPut("lock", body2, ifMatch(etag))
	id, in, _ := g.onlyIntent()
	require.Equal(t, handlers.IntentCommitted, in.State)
	g.rm.lag(id.Key(), g.wm.pendingRow(t))
	g.age()

	g.pass()

	g.requireConverged(id, in, true)
}

// With no leader to decide the fence, a pending row stays as it is and its
// shards are not touched, even when the scan shows it pending and aged. A
// pass with a leader settles it by what the leader holds.
// Verifies: ADR-0003:S7.
func TestS7_ALeaderFailureKeepsThePendingRow(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	crashAfterTheTransaction(g)
	g.condPut("lock", body2, ifMatch(etag))
	id, in, committedRow := g.onlyIntent()
	g.rm.lag(id.Key(), g.wm.pendingRow(t))
	g.rm.failTxns(errors.New("no meta leader"))
	g.age()

	g.pass()

	assert.Equal(t, committedRow, g.intents()[id.Key()], "the row changed with no leader")
	assert.Zero(t, g.rb.total(), "shards were touched with no leader")
	assert.Equal(t, int64(1), g.svc.Stats().IntentsFailed)
	g.requireBody("lock", body2)

	g.rm.failTxns(nil)
	g.pass()
	g.requireConverged(id, in, true)
}

// A pass that scans a live writer's intent as pending and aged, and fences
// only after the writer's own transaction applied, finds it committed: the
// write is committed, answers 200 and reads back.
// Verifies: ADR-0003:S7.
func TestS7_APassThatRacesALiveWriterDoesNotAbortItsCommit(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	g.age()
	writerAtTxn, reaperAtFence, writerApplied := make(chan struct{}), make(chan struct{}), make(chan struct{})
	g.wm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n != 1 {
			return next()
		}
		close(writerAtTxn)
		<-reaperAtFence
		outcome, current, err := next()
		close(writerApplied)
		return outcome, current, err
	})
	g.rm.beforeTxn = func() {
		close(reaperAtFence)
		<-writerApplied
	}
	// The writer's shard commit waits for the pass, so an abort would land first.
	g.wb.commitGate = make(chan struct{})

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- g.condPut("lock", body2, ifMatch(etag)) }()
	<-writerAtTxn
	id, in, _ := g.onlyIntent()
	require.Equal(t, handlers.IntentPending, in.State)
	g.pass()
	select {
	case <-reaperAtFence:
	default:
		close(reaperAtFence)
		close(g.wb.commitGate)
		<-done
		t.Fatal("the pass settled a live writer's pending intent without fencing it")
	}
	close(g.wb.commitGate)
	rr := <-done

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	g.requireConverged(id, in, true)
}

// A pass that fences a slow writer before its transaction makes that
// transaction fail its pending check: the write answers 500 and the previous
// object stays readable.
// Verifies: ADR-0003:S7.
func TestS7_AFencedSlowWriterFailsAndKeepsThePreviousObject(t *testing.T) {
	g := newIntentGate(t)
	etag := g.mustPut("lock", body1)
	g.age()
	var id handlers.IntentID
	var in handlers.Intent
	g.wm.setHook(func(n int, next txnFunc) (meta.TxnOutcome, []meta.TxnValue, error) {
		if n == 1 {
			id, in, _ = g.onlyIntent()
			g.pass()
		}
		return next()
	})

	rr := g.condPut("lock", body2, ifMatch(etag))

	require.Equal(t, http.StatusInternalServerError, rr.Code, "body: %s", rr.Body.String())
	g.requireConverged(id, in, false)
}
