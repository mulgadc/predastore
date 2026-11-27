package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// writeCondition is the precondition a PutObject carries: an If-Match list,
// or If-None-Match: *.
type writeCondition struct {
	ifMatch     string
	ifNoneMatch bool
}

// parseWriteCondition reads a PutObject's precondition. It reports whether
// the request is conditional, and whether it carries one S3 does not evaluate:
// an If-None-Match other than "*", an empty header, or both headers together.
func parseWriteCondition(h http.Header) (cond writeCondition, conditional, unsupported bool) {
	_, hasMatch := h["If-Match"]
	_, hasNoneMatch := h["If-None-Match"]
	switch {
	case !hasMatch && !hasNoneMatch:
		return writeCondition{}, false, false
	case hasMatch && hasNoneMatch:
		return writeCondition{}, true, true
	case hasNoneMatch:
		if strings.TrimSpace(strings.Join(h.Values("If-None-Match"), ",")) != "*" {
			return writeCondition{}, true, true
		}
		return writeCondition{ifNoneMatch: true}, true, false
	default:
		list := strings.Join(h.Values("If-Match"), ",")
		if strings.TrimSpace(list) == "" {
			return writeCondition{}, true, true
		}
		return writeCondition{ifMatch: list}, true, false
	}
}

type conditionVerdict int

const (
	conditionHolds conditionVerdict = iota
	conditionFailed
	conditionNoKey
)

// eval decides the condition against the current object, nil for none.
func (c writeCondition) eval(current *ObjectToShardNodes) conditionVerdict {
	if current == nil {
		if c.ifMatch != "" {
			return conditionNoKey
		}
		return conditionHolds
	}
	p := preconditions{ifMatch: c.ifMatch}
	if c.ifNoneMatch {
		p.ifNoneMatch = "*"
	}
	etag, hasETag := current.ETag()
	if evalPreconditions(etag, hasETag, time.Time{}, false, p) != preconditionsHold {
		return conditionFailed
	}
	return conditionHolds
}

// HeldPut is a held shard write the node has registered, with no body sent.
// Send or Abandon it exactly once.
type HeldPut interface {
	Send(ctx context.Context, body io.Reader) (*blob.PutResponse, error)
	Abandon()
}

var _ HeldPut = (*blob.HeldPut)(nil)

// HeldBlobClient opens held shard writes. A blob client that offers neither
// it nor blob.Client's OpenHeld holds nothing, so a conditional write to it
// is refused.
type HeldBlobClient interface {
	OpenHeldPut(ctx context.Context, nodeID config.NodeID, req blob.PutRequest) (HeldPut, error)
}

type blobHeldOpener interface {
	OpenHeld(ctx context.Context, nodeID config.NodeID, req blob.PutRequest) (*blob.HeldPut, error)
}

var _ blobHeldOpener = (*blob.Client)(nil)

type heldAdapter struct{ c blobHeldOpener }

func (a heldAdapter) OpenHeldPut(ctx context.Context, nodeID config.NodeID, req blob.PutRequest) (HeldPut, error) {
	hp, err := a.c.OpenHeld(ctx, nodeID, req)
	if err != nil {
		return nil, err
	}
	return hp, nil
}

var _ HeldBlobClient = heldAdapter{}

func heldClientOf(bc BlobClient) HeldBlobClient {
	switch c := bc.(type) {
	case HeldBlobClient:
		return c
	case blobHeldOpener:
		return heldAdapter{c}
	}
	return nil
}

// errHoldShortfall is a write that could not hold enough shards because too
// few of its nodes support held writes.
var errHoldShortfall = errors.New("too few blob nodes hold a conditional write")

// conditionalPut is one conditional PutObject after the handler's validation.
type conditionalPut struct {
	mc     MetaClient
	bc     BlobClient
	ring   *placement.Ring
	cfg    Config
	bucket string
	key    string
	cond   writeCondition
	target writeTarget
	place  ObjectToShardNodes
	body   io.Reader
	size   int64
	// seal finishes the body and stamps the record with its digest.
	seal  func(place *ObjectToShardNodes) error
	ebs   bool
	phase time.Time
}

// currentObject is what the transaction checks: the rows a read resolves,
// as the gate evaluated them.
type currentObject struct {
	checks []meta.TxnCheck
	// place is the current object, nil when there is none.
	place *ObjectToShardNodes
	// displaced is the placement row this write replaces, nil for none.
	displaced []byte
}

// conditionalWrite runs a conditional PutObject: evaluate, open held streams,
// write the intent, stream, then commit the metadata in one transaction that
// applies only to the state the condition was evaluated against.
func conditionalWrite(w http.ResponseWriter, r *http.Request, p *conditionalPut) {
	ctx := r.Context()
	cur, err := readCurrentObject(ctx, p.mc, p.bucket, p.key, p.target)
	if err != nil {
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}
	if answerCondition(w, r, p.key, p.cond.eval(cur.place)) {
		return
	}

	id := IntentID{Hash: p.target.hash, Epoch: p.place.WriteEpoch}
	intent := Intent{
		State:     IntentPending,
		Bucket:    p.bucket,
		Key:       p.key,
		VersionID: p.target.versionID,
		Nodes:     p.place.AllNodes(),
		Handoff:   handoffNode(p.ring, p.cfg, p.target.hash),
		Release:   releaseOf(cur.displaced, p.place.WriteEpoch, p.target),
		Created:   time.Now().UTC(),
	}

	// A zero-byte object has no shard, so nothing needs an intent to clean up.
	var pendingRow []byte
	written := fullWidth(p.cfg.TotalShards())
	if p.size > 0 {
		if pendingRow, err = EncodeIntent(intent); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		var intentWritten bool
		written, intentWritten, err = writeHeld(ctx, p, func() error { return p.mc.Put(ctx, id.Key(), pendingRow) })
		recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseShardFanout, p.phase)
		if err != nil {
			slog.ErrorContext(ctx, "Conditional write: shard distribution failed", "error", err)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, writeFailureReason(err))
			if intentWritten {
				abandonIntent(ctx, p, id, intent)
			}
			if errors.Is(err, errHoldShortfall) {
				WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
					"Conditional writes need blob nodes that hold a prepared shard")
				return
			}
			HandleError(w, r, mapPutErr(err))
			return
		}
	}

	place := p.place
	if err := p.seal(&place); err != nil {
		abandonIntentIf(ctx, p, id, intent)
		HandleError(w, r, err)
		return
	}
	record, err := EncodePlacement(place)
	if err != nil {
		abandonIntentIf(ctx, p, id, intent)
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
		return
	}

	for mismatches := 0; ; {
		committed := intent.withState(IntentCommitted)
		committed.Release = releaseOf(cur.displaced, place.WriteEpoch, p.target)
		checks, writes, err := conditionalTxn(p, id, cur, record, place, pendingRow, committed)
		if err != nil {
			abandonIntentIf(ctx, p, id, intent)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
			return
		}
		phase := time.Now()
		outcome, current, err := p.mc.Txn(ctx, checks, writes)
		recordPhase(ctx, telemetry.GateOpPut, telemetry.PhaseMetaPlacement, phase)

		switch outcome {
		case meta.TxnApplied:
			finishCommitted(ctx, w, r, p, id, committed, written, place)
			return

		case meta.TxnRefused:
			slog.WarnContext(ctx, "Conditional write refused by meta", "error", err)
			abandonIntentIf(ctx, p, id, intent)
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
			WriteS3Error(w, r, http.StatusServiceUnavailable, "SlowDown", "Please reduce your request rate.")
			return

		case meta.TxnMismatch:
			if p.size > 0 && !holdsValue(current, id.Key(), pendingRow) {
				// Fenced: the reaper decided this write, so it can never publish.
				abandonIntent(ctx, p, id, intent)
				telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, "the write was fenced before it committed", 500))
				return
			}
			mismatches++
			if cur, err = readCurrentObject(ctx, p.mc, p.bucket, p.key, p.target); err != nil {
				abandonIntentIf(ctx, p, id, intent)
				HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
				return
			}
			verdict := p.cond.eval(cur.place)
			// A held If-None-Match after a mismatch means a create and a delete
			// ran during the write; a held If-Match that keeps meeting changes
			// gives up after three.
			if verdict == conditionHolds && (p.cond.ifNoneMatch || mismatches >= maxConditionalMismatches) {
				abandonIntentIf(ctx, p, id, intent)
				WriteS3Error(w, r, http.StatusConflict, "ConditionalRequestConflict",
					"A conflicting operation occurred. If using PutObject you can retry the request.")
				return
			}
			if verdict != conditionHolds {
				abandonIntentIf(ctx, p, id, intent)
				answerCondition(w, r, p.key, verdict)
				return
			}

		default:
			slog.WarnContext(ctx, "Conditional write outcome unknown; fencing its intent", "error", err)
			resolveUnknown(ctx, w, r, p, id, intent, pendingRow, written, place)
			return
		}
	}
}

// maxConditionalMismatches is how many mismatches an If-Match write that
// still holds meets before it answers 409.
const maxConditionalMismatches = 3

// resolveUnknown settles a transaction whose outcome is unknown through the
// log: the fence either aborts the intent, or finds that the write committed.
func resolveUnknown(
	ctx context.Context, w http.ResponseWriter, r *http.Request, p *conditionalPut,
	id IntentID, intent Intent, pendingRow []byte, written writeResult, place ObjectToShardNodes,
) {
	if p.size == 0 {
		telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, "metadata outcome unknown", 500))
		return
	}
	result, decided, err := FenceIntent(ctx, p.mc, id, pendingRow)
	if result == FenceCommitted {
		finishCommitted(ctx, w, r, p, id, decided, written, place)
		return
	}
	telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonMeta)
	switch result {
	case FenceApplied, FenceAborted:
		abandonIntent(ctx, p, id, intent)
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, "metadata outcome unknown; the write was aborted", 500))
	default:
		slog.ErrorContext(ctx, "Conditional write left undecided for the reaper", "result", result, "error", err)
		HandleError(w, r, model.NewS3Error(model.ErrInternalError, "metadata outcome unknown", 500))
	}
}

// finishCommitted commits the shards of a write whose metadata is published,
// and answers 200 once they are durable at the floor. Below it the object is
// still readable, because a read commits the held shards it names.
func finishCommitted(
	ctx context.Context, w http.ResponseWriter, r *http.Request, p *conditionalPut,
	id IntentID, committed Intent, written writeResult, place ObjectToShardNodes,
) {
	if p.size > 0 {
		res := CommitIntent(ctx, p.bc, id, committed)
		if res.Complete {
			deleteIntent(ctx, p.mc, id)
		}
		if res.Overtaken > 0 {
			slog.InfoContext(ctx, "A newer write overtook this one",
				"shards_superseded", res.Overtaken, "epoch", fmt.Sprintf("%016x", id.Epoch))
		}
		if res.Durable < p.cfg.MinShards() {
			telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeFailed, telemetry.WriteReasonShardWrite)
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, fmt.Sprintf(
				"%v: %d of %d at epoch %016x", errCommitFloor, res.Durable, p.cfg.MinShards(), id.Epoch), 500))
			return
		}
		written.committed = res.Durable
	}
	telemetry.RecordObjectWrite(ctx, telemetry.WriteOutcomeSuccess, "")
	answerPut(w, r, written, place, p.ebs, p.target.versionID)
}

// abandonIntentIf aborts a write that has an intent, which is every write with
// a body.
func abandonIntentIf(ctx context.Context, p *conditionalPut, id IntentID, intent Intent) {
	if p.size > 0 {
		abandonIntent(ctx, p, id, intent)
	}
}

// abandonIntent aborts the write's shards on every candidate, and deletes the
// intent only once each one confirmed. Otherwise the reaper finishes it.
func abandonIntent(ctx context.Context, p *conditionalPut, id IntentID, intent Intent) {
	if AbortIntent(ctx, p.bc, id, intent) {
		deleteIntent(ctx, p.mc, id)
	}
}

func deleteIntent(ctx context.Context, mc MetaClient, id IntentID) {
	if err := mc.Delete(ctx, id.Key()); err != nil {
		slog.WarnContext(ctx, "Settled intent not deleted; the reaper will",
			"epoch", fmt.Sprintf("%016x", id.Epoch), "err", err)
	}
}

// answerCondition writes the 412 or 404 a failed condition calls for, and
// reports whether it did.
func answerCondition(w http.ResponseWriter, r *http.Request, key string, verdict conditionVerdict) bool {
	switch verdict {
	case conditionFailed:
		WriteS3Error(w, r, http.StatusPreconditionFailed, "PreconditionFailed", preconditionFailedMessage)
		return true
	case conditionNoKey:
		HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return true
	default:
		return false
	}
}

// holdsValue reports whether a mismatch's answer shows key holding want.
func holdsValue(current []meta.TxnValue, key string, want []byte) bool {
	for _, v := range current {
		if string(v.Key) == key {
			return v.Exists && bytes.Equal(v.Value, want)
		}
	}
	return false
}

// releaseOf is the generation a write displaces. A versioned write displaces
// nothing: the version it follows is a different shard set and stays live.
func releaseOf(displaced []byte, mine uint64, target writeTarget) *IntentRelease {
	if target.versioned || len(displaced) == 0 {
		return nil
	}
	prev, err := DecodePlacement(displaced)
	if err != nil || prev.WriteEpoch == mine || prev.Size == 0 {
		return nil
	}
	return &IntentRelease{Epoch: prev.WriteEpoch, Nodes: prev.AllNodes()}
}

// readCurrentObject reads, from the leader, the rows that define the current
// object of a key, and the checks that pin them. The write target says which
// bucket state the write was placed for, so the two cannot disagree.
func readCurrentObject(ctx context.Context, mc MetaClient, bucket, key string, target writeTarget) (currentObject, error) {
	var cur currentObject
	nullHash := model.ObjectHash(bucket, key)
	nullKey := TableKey(model.TableObjects, string(nullHash[:]))

	// Never versioned: the placement row at the object hash is the object.
	if target.versionID == "" {
		row, err := cur.pin(ctx, mc, nullKey)
		if err != nil {
			return currentObject{}, err
		}
		cur.displaced = row
		return cur, cur.setPlace(row)
	}

	// Versioned: the listing key names the current version, and its absence
	// is no current object, whether a delete marker hides the key or not.
	listing, err := cur.pin(ctx, mc, TableKey(model.TableObjects, objectARN(bucket, key)))
	if err != nil {
		return currentObject{}, err
	}
	var namedKey string
	if len(listing) == sha256.Size {
		namedKey = TableKey(model.TableObjects, string(listing))
		row, err := cur.pin(ctx, mc, namedKey)
		if err != nil {
			return currentObject{}, err
		}
		if err := cur.setPlace(row); err != nil {
			return currentObject{}, err
		}
		if namedKey == nullKey {
			cur.displaced = row
		}
	}

	if target.versioned {
		newKey := TableKey(model.TableObjects, string(target.hash[:]))
		cur.checks = append(cur.checks, meta.TxnCheck{Key: []byte(newKey), Absent: true})
		return cur, nil
	}
	// Suspended: the write replaces the null version, whatever is current.
	if namedKey != nullKey {
		row, err := cur.pin(ctx, mc, nullKey)
		if err != nil {
			return currentObject{}, err
		}
		cur.displaced = row
	}
	return cur, nil
}

// pin reads one row from the leader and adds a check that it is unchanged.
func (c *currentObject) pin(ctx context.Context, mc MetaClient, stored string) ([]byte, error) {
	row, err := mc.LeaderGet(ctx, stored)
	switch {
	case errors.Is(err, meta.ErrNotFound):
		c.checks = append(c.checks, meta.TxnCheck{Key: []byte(stored), Absent: true})
		return nil, nil
	case err != nil:
		return nil, err
	}
	c.checks = append(c.checks, meta.TxnCheck{Key: []byte(stored), Value: row})
	return row, nil
}

func (c *currentObject) setPlace(row []byte) error {
	if row == nil {
		return nil
	}
	place, err := DecodePlacement(row)
	if err != nil {
		return err
	}
	c.place = &place
	return nil
}

// conditionalTxn builds the transaction: the object checks, the intent still
// pending, and the writes that publish the object and commit its intent.
func conditionalTxn(
	p *conditionalPut, id IntentID, cur currentObject, record []byte, place ObjectToShardNodes,
	pendingRow []byte, committed Intent,
) ([]meta.TxnCheck, []meta.TxnWrite, error) {
	checks := cur.checks
	writes := []meta.TxnWrite{
		{Key: []byte(TableKey(model.TableObjects, string(p.target.hash[:]))), Value: record},
	}
	if p.target.versioned {
		rec := versionRecordOf(p.target, place)
		row, err := encodeVersionRecord(rec)
		if err != nil {
			return nil, nil, err
		}
		writes = append(writes, meta.TxnWrite{
			Key:   []byte(TableKey(model.TableObjectVersions, versionIndexKey(p.bucket, p.key, rec.WriteEpoch, rec.VersionID))),
			Value: row,
		})
	}
	writes = append(writes, meta.TxnWrite{
		Key: []byte(TableKey(model.TableObjects, objectARN(p.bucket, p.key))), Value: p.target.hash[:],
	})
	if p.size > 0 {
		committedRow, err := EncodeIntent(committed)
		if err != nil {
			return nil, nil, err
		}
		checks = append(slices.Clip(checks), meta.TxnCheck{Key: []byte(id.Key()), Value: pendingRow})
		writes = append(writes, meta.TxnWrite{Key: []byte(id.Key()), Value: committedRow})
	}
	return checks, writes, nil
}

// writeHeld writes the body as held shards. Every owner's stream is open and
// registered before writeIntent runs, and no body byte moves before it
// returns. It reports whether writeIntent succeeded, which is when the
// caller owns the cleanup of the candidates.
func writeHeld(ctx context.Context, p *conditionalPut, writeIntent func() error) (writeResult, bool, error) {
	cfg, total := p.cfg, p.cfg.TotalShards()
	nodes := p.place.AllNodes()
	lay := newLayout(cfg.DataShards, p.size, p.place.BlockSize)
	if cfg.DataShards == 1 && cfg.ParityShards == 0 {
		lay.shardSize = p.size
	}

	held := heldClientOf(p.bc)
	puts := make([]HeldPut, total)
	errs := make([]error, total)
	if held == nil {
		for i := range errs {
			errs[i] = blob.ErrHoldUnsupported
		}
	} else {
		var wg sync.WaitGroup
		for i := range total {
			wg.Go(func() {
				puts[i], errs[i] = held.OpenHeldPut(ctx, nodes[i], blob.PutRequest{
					Key: p.target.hash, Size: lay.shardSize, Epoch: p.place.WriteEpoch,
					Index: uint32(i), //nolint:gosec // G115: index bounded by the shard count.
				})
			})
		}
		wg.Wait()
	}

	registered, unsupported := 0, 0
	var firstErr error
	for i, err := range errs {
		switch {
		case err == nil:
			registered++
		case errors.Is(err, blob.ErrHoldUnsupported):
			unsupported++
		}
		if err != nil {
			logShardWriteFailure(ctx, nodes[i], i, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	abandonAll := func() {
		for _, hp := range puts {
			if hp != nil {
				hp.Abandon()
			}
		}
	}
	if registered < cfg.MinShards() {
		abandonAll()
		return writeResult{landed: make([]bool, total)}, false, shortfall(registered, unsupported, cfg, firstErr)
	}
	if err := writeIntent(); err != nil {
		abandonAll()
		return writeResult{landed: make([]bool, total)}, false, fmt.Errorf("write intent: %w", err)
	}

	if cfg.DataShards == 1 && cfg.ParityShards == 0 {
		return sendSingleHeld(ctx, puts[0], nodes[0], p.body, p.size)
	}

	streams := make([]*shardStream, total)
	var refused []int
	for i := range total {
		if puts[i] == nil {
			streams[i] = failedShardStream(i, nodes[i], errs[i])
			refused = append(refused, i)
			continue
		}
		streams[i] = startHeldStream(ctx, puts[i], i, nodes[i])
	}
	result, err := pumpStripes(ctx, p.bc, cfg, p.target.hash, p.place.WriteEpoch, streams, p.body, lay, p.size,
		handoffNode(p.ring, cfg, p.target.hash), refused)
	if err != nil {
		return result, true, err
	}
	result, firstErr = gatherStreams(ctx, streams, result)
	if result.holding < cfg.MinShards() {
		unheld := 0
		for _, err := range errs {
			if errors.Is(err, blob.ErrHoldUnsupported) {
				unheld++
			}
		}
		unheld += result.landedCount() - result.holding
		return result, true, shortfall(result.holding, unheld, cfg, firstErr)
	}
	return result, true, nil
}

// shortfall explains a held write below the floor: a refusal when the nodes
// without held writes are what it lacks, and a write failure otherwise.
func shortfall(holding, unheld int, cfg Config, firstErr error) error {
	if holding+unheld >= cfg.MinShards() {
		return fmt.Errorf("%w: %d of %d held", errHoldShortfall, holding, cfg.MinShards())
	}
	return fmt.Errorf("%w: %d of %d shards held: %w", errShardWriteFloor, holding, cfg.MinShards(), firstErr)
}

// sendSingleHeld streams an unsplit object to its one node.
func sendSingleHeld(ctx context.Context, hp HeldPut, node config.NodeID, body io.Reader, size int64) (writeResult, bool, error) {
	counted := &countingReader{r: body}
	result := writeResult{landed: []bool{false}, holders: []config.NodeID{node}}
	resp, err := hp.Send(ctx, counted)
	if err != nil {
		logShardWriteFailure(ctx, node, 0, err)
		result.missing = []config.NodeID{node}
		return result, true, err
	}
	result.landed[0], result.poolNearFull = true, resp.PoolNearFull
	if counted.n != size {
		return result, true, fmt.Errorf("body delivered %d bytes, declared %d", counted.n, size)
	}
	if !resp.Holding {
		return result, true, fmt.Errorf("%w: 0 of 1 held", errHoldShortfall)
	}
	result.holding = 1
	return result, true, nil
}

// failedShardStream is a position whose held stream never opened. Its outcome
// is already waiting, so handoff and collection treat it as refused.
func failedShardStream(index int, node config.NodeID, err error) *shardStream {
	s := &shardStream{index: index, owner: node, holder: node, done: make(chan shardWriteOutcome, 1), failed: true}
	s.done <- shardWriteOutcome{shardIndex: index, err: err}
	return s
}

// startHeldStream carries one shard over a held put that is registered.
func startHeldStream(ctx context.Context, hp HeldPut, index int, node config.NodeID) *shardStream {
	pr, pw := io.Pipe()
	s := &shardStream{index: index, owner: node, holder: node, pw: pw, done: make(chan shardWriteOutcome, 1)}
	go func() {
		resp, err := hp.Send(ctx, pr)
		_ = pr.CloseWithError(io.ErrClosedPipe)
		if err != nil {
			s.done <- shardWriteOutcome{shardIndex: index, err: err}
			return
		}
		s.done <- shardWriteOutcome{shardIndex: index, poolNearFull: resp.PoolNearFull, holding: resp.Holding}
	}()
	return s
}
