package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/telemetry"
)

// ErrNotFound is returned by reads when no replica holds the key.
var ErrNotFound = errors.New("key not found")

// ErrNotLeader is returned by a Server write this replica cannot commit, and
// by client writes no replica would accept as leader, which usually means an
// election is still settling.
var ErrNotLeader = errors.New("meta replica is not the leader")

// ErrUndelivered marks an attempt that never reached a replica, so the command
// it carried cannot have been applied and another attempt is safe. Every other
// failure is ambiguous: a command whose response was lost is in the log.
var ErrUndelivered = errors.New("meta request not delivered")

// Item is one key-value pair returned by a scan.
type Item struct {
	Key   string
	Value []byte
}

// Client reads and writes global state over rpc streams, hiding the wire
// protocol from callers. The replicas are a plain key-value store: keys are
// opaque, taken as strings that may hold arbitrary bytes, and any namespacing
// belongs to the caller.
//
// Reads try the cached leader then every replica; writes follow not-leader
// redirects and cache the leader they land on. A replica that fails an attempt
// is ordered last until it answers again, so one unresponsive replica costs a
// deadline once rather than once per operation.
type Client struct {
	rpc        *rpc.Client
	replicas   []config.NodeID
	timeout    time.Duration
	maxRetries int

	// now is the clock the cooldown is judged against. Production reads the
	// wall clock; a test supplies its own so expiry does not take 30s.
	now func() time.Time

	mu           sync.Mutex
	leader       config.NodeID // cached leader replica id; 0 means unknown
	unresponsive map[config.NodeID]time.Time
}

// replicaCooldown is how long a replica that failed an attempt is ordered
// last. It has to outlast the gap between two operations for the second to
// benefit from what the first learned, and demotion only ever means tried after
// the others, so erring long is cheap.
const replicaCooldown = 30 * time.Second

// ClientConfig configures a Client.
type ClientConfig struct {
	// Client carries the streams; it owns the mapping from node id to
	// address, so this client only ever names replicas by id.
	Client *rpc.Client
	// Replicas lists the meta replica node ids.
	Replicas []config.NodeID
	// Timeout is a fallback deadline layered on the caller's context, so an
	// attempt against an unresponsive replica cannot hang forever. The
	// caller's own cancellation still wins. Default 10s.
	Timeout time.Duration
	// MaxRetries bounds write retry rounds across the replica set.
	// Default 3.
	MaxRetries int
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("missing rpc client")
	}
	if len(cfg.Replicas) == 0 {
		return nil, fmt.Errorf("no meta replicas")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	return &Client{
		rpc:          cfg.Client,
		replicas:     cfg.Replicas,
		timeout:      cfg.Timeout,
		maxRetries:   cfg.MaxRetries,
		now:          time.Now,
		unresponsive: make(map[config.NodeID]time.Time),
	}, nil
}

// call performs one request round trip against a replica: header, optional
// body, half-close, then the response envelope. The configured timeout bounds
// the attempt only as a fallback; cancelling ctx aborts it sooner.
//
// Whether the replica answered is recorded either way, which is what stops the
// next operation leading with a replica this one has just waited out.
func (c *Client) call(ctx context.Context, target config.NodeID, op rpc.Opcode, req *MetaRequest, body []byte) (resp *MetaResponse, err error) {
	caller := ctx
	defer func() { c.observeReplica(caller, target, err) }()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	stream, err := rpc.OpenStream(ctx, c.rpc, target, op, req)
	if err != nil {
		// Nothing left the client, so the command cannot have been applied.
		return nil, fmt.Errorf("open stream to replica %d: %w: %w", target, ErrUndelivered, err)
	}
	if len(body) > 0 {
		if _, err := stream.Write(body); err != nil {
			stream.CancelRead(0)
			stream.CancelWrite(0)
			return nil, fmt.Errorf("write body to replica %d: %w", target, err)
		}
	}
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("half-close stream to replica %d: %w", target, err)
	}

	// The response can block until the deadline; abort the read side when
	// the context fires so the decode does not outlive the attempt.
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(0) })
	defer stop()

	var decoded MetaResponse
	if err := json.NewDecoder(stream).Decode(&decoded); err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("decode response from replica %d: %w", target, err)
	}

	// Decode stops at the end of the JSON value, which leaves the FIN unread
	// and the stream never completed. The guard above still bounds this, so a
	// replica that answers and then goes quiet cannot hold the call open.
	if _, err := io.Copy(io.Discard, stream); err != nil {
		stream.CancelRead(0)
	}
	return &decoded, nil
}

// observeReplica records whether a replica answered. A caller that has given up
// is not evidence about any replica: its cancellation fails every attempt in
// flight, and marking on it would demote the whole set for one abandoned
// request.
func (c *Client) observeReplica(caller context.Context, target config.NodeID, err error) {
	if err != nil && caller.Err() != nil {
		return
	}

	c.mu.Lock()
	_, wasCooling := c.unresponsive[target]
	if err == nil {
		delete(c.unresponsive, target)
	} else {
		c.unresponsive[target] = c.now()
	}
	leader := c.leader
	c.mu.Unlock()

	// Only the transitions, because the steady states are the common ones: a
	// replica answering, or one that has already been demoted failing again.
	switch {
	case err != nil && !wasCooling:
		slog.WarnContext(caller, "Meta replica did not answer; ordering it last",
			"replica", target, "was_cached_leader", leader == target,
			"cooldown_ms", replicaCooldown.Milliseconds(), "err", err)
	case err == nil && wasCooling:
		slog.InfoContext(caller, "Meta replica answered again", "replica", target)
	}
}

// readOrder returns the replicas that answered most recently first, cached
// leader ahead of the rest, and any replica still inside its cooldown last.
//
// Last, never absent. Reads consult every replica before giving up and writes
// rotate through all of them, so this changes how long an answer takes and not
// which answers are reachable — which is what makes a stale mark harmless.
func (c *Client) readOrder() []config.NodeID {
	c.mu.Lock()
	leader := c.leader
	cooling := make(map[config.NodeID]bool, len(c.unresponsive))
	for id, at := range c.unresponsive {
		if c.now().Sub(at) < replicaCooldown {
			cooling[id] = true
		} else {
			delete(c.unresponsive, id)
		}
	}
	c.mu.Unlock()

	order := make([]config.NodeID, 0, len(c.replicas))
	if leader != 0 && !cooling[leader] {
		order = append(order, leader)
	}
	for _, id := range c.replicas {
		if id != leader && !cooling[id] {
			order = append(order, id)
		}
	}
	for _, id := range c.replicas {
		if cooling[id] {
			order = append(order, id)
		}
	}

	return order
}

func (c *Client) cacheLeader(id config.NodeID) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

// observeOp records one meta operation and how long it took, counting every
// replica the attempt had to try. Deferred from the exported methods rather
// than from call, so a read that tried three replicas is one operation.
//
// Not-found is its own outcome. It is the answer to a question, not a failure,
// and counting it as one would make a HEAD of a missing object look like a
// broken meta plane.
func observeOp(ctx context.Context, op string, start time.Time, err *error) {
	outcome := telemetry.OutcomeSuccess
	switch {
	case *err == nil:
	case errors.Is(*err, ErrNotFound):
		outcome = telemetry.MetaOutcomeNotFound
	default:
		outcome = telemetry.OutcomeError
	}
	telemetry.RecordMetaClientOp(ctx, op, outcome, time.Since(start).Seconds())
}

// request builds a wire header for a key. Callers hand keys over as strings;
// the wire carries them as bytes so binary keys survive JSON.
func request(key string, limit int) *MetaRequest {
	return &MetaRequest{Key: []byte(key), Limit: limit}
}

// Get retrieves a value. A replica that has not applied the key yet answers
// not-found, so every replica is consulted before giving up.
func (c *Client) Get(ctx context.Context, key string) (value []byte, err error) {
	defer observeOp(ctx, telemetry.MetaOpGet, time.Now(), &err)

	var lastErr error
	notFound := false
	for _, id := range c.readOrder() {
		resp, err := c.call(ctx, id, OpMetaGet, request(key, 0), nil)
		if err != nil {
			lastErr = err
			continue
		}
		switch resp.Err {
		case "":
			return resp.Value, nil
		case ErrCodeNotFound:
			notFound = true
		default:
			lastErr = fmt.Errorf("replica %d: %s", id, resp.Err)
		}
	}
	if notFound {
		return nil, fmt.Errorf("get %q: %w", key, ErrNotFound)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("get %q: no replica answered", key)
}

// Exists reports whether the key is present.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Scan lists up to limit key-value pairs with the prefix, preferring the
// leader for freshness but accepting any replica. A limit of zero or less
// returns every match. Keys come back exactly as stored.
func (c *Client) Scan(ctx context.Context, prefix string, limit int) ([]Item, error) {
	return c.ScanFrom(ctx, prefix, "", limit)
}

// ScanFrom lists up to limit key-value pairs with the prefix that sort strictly
// after the cursor, which is how a caller pages through a prefix too large to
// ask for at once. Passing back the last key of a page continues from it; an
// empty cursor starts at the beginning.
//
// Pages are not a consistent snapshot: the table is written while a scan runs,
// so a key inserted behind the cursor is missed and one deleted ahead of it is
// never seen. Every caller so far treats a pass as a sweep to be repeated
// rather than an inventory, which is what makes that acceptable.
func (c *Client) ScanFrom(ctx context.Context, prefix, after string, limit int) (items []Item, err error) {
	defer observeOp(ctx, telemetry.MetaOpScan, time.Now(), &err)

	var lastErr error
	for _, id := range c.readOrder() {
		req := request(prefix, limit)
		req.After = []byte(after)
		resp, err := c.call(ctx, id, OpMetaScan, req, nil)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Err != "" {
			lastErr = fmt.Errorf("replica %d: %s", id, resp.Err)
			continue
		}
		items := make([]Item, len(resp.Items))
		for i, it := range resp.Items {
			items[i] = Item{Key: string(it.Key), Value: it.Value}
		}
		return items, nil
	}
	return nil, lastErr
}

// ErrNoLeaderRead reports that no replica would serve a leader read, which
// means there is no leader or it could not confirm itself in time.
var ErrNoLeaderRead = errors.New("no meta replica would serve a leader read")

// LeaderGet is Get answered only by a leader that has applied everything
// committed. A follower's not-found is not trusted, so neither is its value.
func (c *Client) LeaderGet(ctx context.Context, key string) (value []byte, err error) {
	return c.getFromLeader(ctx, OpMetaGet, key)
}

// AuthorityGet requires the leader to confirm a fresh quorum before reading.
func (c *Client) AuthorityGet(ctx context.Context, key string) ([]byte, error) {
	return c.getFromLeader(ctx, OpMetaAuthorityGet, key)
}

func (c *Client) getFromLeader(ctx context.Context, op rpc.Opcode, key string) (value []byte, err error) {
	defer observeOp(ctx, telemetry.MetaOpGet, time.Now(), &err)

	resp, err := c.leaderRead(ctx, op, request(key, 0))
	if err != nil {
		return nil, err
	}
	switch resp.Err {
	case "":
		return resp.Value, nil
	case ErrCodeNotFound:
		return nil, fmt.Errorf("get %q: %w", key, ErrNotFound)
	default:
		return nil, errors.New(resp.Err)
	}
}

// LeaderScanFrom is ScanFrom answered only by a leader that has applied
// everything committed. Each page is fresh; the pages are still not a snapshot.
func (c *Client) LeaderScanFrom(ctx context.Context, prefix, after string, limit int) (items []Item, err error) {
	defer observeOp(ctx, telemetry.MetaOpScan, time.Now(), &err)

	req := request(prefix, limit)
	req.After = []byte(after)
	resp, err := c.leaderRead(ctx, OpMetaScan, req)
	if err != nil {
		return nil, err
	}
	if resp.Err != "" {
		return nil, errors.New(resp.Err)
	}
	items = make([]Item, len(resp.Items))
	for i, it := range resp.Items {
		items[i] = Item{Key: string(it.Key), Value: it.Value}
	}

	return items, nil
}

// LeaderLastKey reads the greatest key in a prefix at the leader's read point.
// Confirmation distinguishes an empty result from a reply by an older server.
func (c *Client) LeaderLastKey(ctx context.Context, prefix string) (key string, err error) {
	defer observeOp(ctx, telemetry.MetaOpScan, time.Now(), &err)

	req := request(prefix, 1)
	req.Last = true
	resp, err := c.leaderRead(ctx, OpMetaLastKey, req)
	if err != nil {
		return "", err
	}
	if resp.Err != "" {
		return "", errors.New(resp.Err)
	}
	if !resp.Last || len(resp.Items) > 1 {
		return "", fmt.Errorf("%w: leader did not confirm a single last-key lookup", ErrNoLeaderRead)
	}
	if len(resp.Items) == 0 {
		return "", nil
	}
	key = string(resp.Items[0].Key)
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("%w: last-key result outside requested prefix", ErrNoLeaderRead)
	}
	return key, nil
}

// leaderRead sends a leader read to each replica in turn until one serves it.
// Any answer other than a refusal came from the leader, so it is cached.
func (c *Client) leaderRead(ctx context.Context, op rpc.Opcode, req *MetaRequest) (*MetaResponse, error) {
	req.Leader = true
	var lastErr error
	for _, id := range c.readOrder() {
		resp, err := c.call(ctx, id, op, req, nil)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Err == ErrCodeNotLeader || resp.Err == ErrCodeBehind {
			lastErr = fmt.Errorf("replica %d: %s", id, resp.Err)
			continue
		}
		c.cacheLeader(id)

		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no replica answered")
	}

	return nil, fmt.Errorf("%w: %w", ErrNoLeaderRead, lastErr)
}

// ListKeys returns every key with the prefix.
func (c *Client) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	items, err := c.Scan(ctx, prefix, 0)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = item.Key
	}
	return keys, nil
}

// Status queries target directly for its raft state. Unlike Get and Scan, it
// consults exactly the replica named: it never tries another replica and
// never follows a not-leader redirect, because the caller is asking what
// this specific process observes, not for a leader-consistent answer.
func (c *Client) Status(ctx context.Context, target config.NodeID) (status MetaStatus, err error) {
	defer observeOp(ctx, telemetry.MetaOpStatus, time.Now(), &err)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	stream, err := rpc.OpenStream(ctx, c.rpc, target, OpMetaStatus, &MetaStatusRequest{})
	if err != nil {
		return MetaStatus{}, fmt.Errorf("open stream to replica %d: %w", target, err)
	}
	if err := stream.Close(); err != nil {
		return MetaStatus{}, fmt.Errorf("half-close stream to replica %d: %w", target, err)
	}

	// The response can block until the deadline; abort the read side when
	// the context fires so the decode does not outlive the attempt.
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(0) })
	defer stop()

	if err := json.NewDecoder(stream).Decode(&status); err != nil {
		stream.CancelRead(0)
		return MetaStatus{}, fmt.Errorf("decode status from replica %d: %w", target, err)
	}
	return status, nil
}

// Put stores a key-value pair through the leader.
func (c *Client) Put(ctx context.Context, key string, value []byte) (err error) {
	defer observeOp(ctx, telemetry.MetaOpPut, time.Now(), &err)
	return c.write(ctx, OpMetaPut, request(key, 0), value)
}

// PutMax publishes a placement unless a newer epoch is already present.
func (c *Client) PutMax(ctx context.Context, key string, value []byte, epoch uint64) (err error) {
	defer observeOp(ctx, telemetry.MetaOpPut, time.Now(), &err)
	req := request(key, 0)
	req.Epoch = epoch
	return c.write(ctx, OpMetaPutMax, req, value)
}

// Swap writes a value and returns the one it replaced, empty if the key was
// absent. It is how a writer of a placement record learns which generation it
// superseded, so the shards behind that generation can be released rather than
// waiting out the retention window.
func (c *Client) Swap(ctx context.Context, key string, value []byte) (previous []byte, err error) {
	defer observeOp(ctx, telemetry.MetaOpPut, time.Now(), &err)

	return c.writeValue(ctx, OpMetaSwap, request(key, 0), value)
}

// TxnOutcome is how a conditional transaction ended, as far as the client can
// know. The zero value is TxnUnknown, so an unset outcome is never read as a
// definite one.
type TxnOutcome int

const (
	// TxnUnknown: the request left the client and no answer proves whether
	// it applied. Only the log can tell.
	TxnUnknown TxnOutcome = iota
	// TxnApplied: every check held and every write is published.
	TxnApplied
	// TxnMismatch: a check failed and no write is published.
	TxnMismatch
	// TxnRefused: nothing was proposed, so nothing can apply.
	TxnRefused
)

// ErrTxnRefused wraps the error of a transaction that was never proposed.
var ErrTxnRefused = errors.New("meta transaction refused")

// ErrTxnUnknown wraps the error of a transaction whose outcome is unknown.
var ErrTxnUnknown = errors.New("meta transaction outcome unknown")

// Txn applies writes through the leader only if every check holds. It sends to
// each replica at most once and moves on only after an undelivered send or a
// not-leader refusal, going to the named leader first; ambiguity is TxnUnknown.
func (c *Client) Txn(ctx context.Context, checks []TxnCheck, writes []TxnWrite) (outcome TxnOutcome, current []TxnValue, err error) {
	defer observeOp(ctx, telemetry.MetaOpTxn, time.Now(), &err)

	if err := ValidateTxn(checks, writes); err != nil {
		return TxnRefused, nil, fmt.Errorf("%w: %w", ErrTxnRefused, err)
	}
	body, err := json.Marshal(TxnRequest{Checks: checks, Writes: writes})
	if err != nil {
		return TxnRefused, nil, fmt.Errorf("%w: encode: %w", ErrTxnRefused, err)
	}
	if len(body) > MaxTxnBodyBytes {
		return TxnRefused, nil, fmt.Errorf("%w: %w: body of %d bytes exceeds the limit of %d",
			ErrTxnRefused, ErrInvalidTxn, len(body), MaxTxnBodyBytes)
	}

	order := c.readOrder()
	tried := make(map[config.NodeID]bool, len(order))
	var lastErr error
	var hint config.NodeID
	for {
		target, ok := nextTxnTarget(order, tried, hint)
		if !ok {
			break
		}
		tried[target] = true
		hint = 0

		resp, err := c.call(ctx, target, OpMetaTxn, &MetaRequest{}, body)
		switch {
		case errors.Is(err, ErrUndelivered):
			lastErr = err
		case err != nil:
			return TxnUnknown, nil, fmt.Errorf("%w: %w", ErrTxnUnknown, err)
		case resp.Err == ErrCodeNotLeader:
			// A follower refuses before it proposes, so sending again is safe.
			lastErr = fmt.Errorf("replica %d: %w", target, ErrNotLeader)
			if id, perr := ParseRaftAddress(resp.Leader); perr == nil {
				c.cacheLeader(id)
				telemetry.RecordMetaRedirect(ctx, telemetry.RedirectNotLeader)
				hint = id
			}
		default:
			return c.txnAnswer(target, resp)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no replica to send to")
	}
	return TxnRefused, nil, fmt.Errorf("%w: %w", ErrTxnRefused, lastErr)
}

// nextTxnTarget picks the hinted leader if it is untried, else the first
// untried replica in order.
func nextTxnTarget(order []config.NodeID, tried map[config.NodeID]bool, hint config.NodeID) (config.NodeID, bool) {
	if hint != 0 && !tried[hint] {
		return hint, true
	}
	for _, id := range order {
		if !tried[id] {
			return id, true
		}
	}
	return 0, false
}

// txnAnswer maps one replica's answer onto an outcome. Any error envelope
// other than a refusal may follow a proposal, so it is unknown.
func (c *Client) txnAnswer(target config.NodeID, resp *MetaResponse) (TxnOutcome, []TxnValue, error) {
	switch {
	case resp.Err == "" && resp.Mismatch:
		c.cacheLeader(target)
		return TxnMismatch, resp.Current, nil
	case resp.Err == "":
		c.cacheLeader(target)
		return TxnApplied, nil, nil
	case resp.Err == ErrCodeInvalid:
		return TxnRefused, nil, fmt.Errorf("%w: replica %d: %s", ErrTxnRefused, target, resp.Err)
	default:
		return TxnUnknown, nil, fmt.Errorf("%w: replica %d: %s", ErrTxnUnknown, target, resp.Err)
	}
}

// Delete removes a key through the leader.
func (c *Client) Delete(ctx context.Context, key string) (err error) {
	defer observeOp(ctx, telemetry.MetaOpDelete, time.Now(), &err)
	return c.write(ctx, OpMetaDelete, request(key, 0), nil)
}

// write drives a consensus write to the leader, following not-leader
// redirects and rotating through replicas while an election settles.
func (c *Client) write(ctx context.Context, op rpc.Opcode, req *MetaRequest, body []byte) error {
	_, err := c.writeValue(ctx, op, req, body)

	return err
}

// writeValue is write, plus whatever the replica answered with. Only a swap
// populates that; every other write leaves it nil.
//
// Put, PutMax and Delete are idempotent, so an attempt whose outcome is
// unknown is simply tried again. A swap is not: it retries only on an attempt
// that never reached a replica, or a refusal that names a leader.
func (c *Client) writeValue(ctx context.Context, op rpc.Opcode, req *MetaRequest, body []byte) ([]byte, error) {
	if IsReservedKey(req.Key) {
		return nil, fmt.Errorf("write %q: %w", req.Key, ErrReservedKey)
	}
	candidates := c.readOrder()
	next := 0
	target := candidates[next]

	var lastErr error
	attempts := c.maxRetries * len(c.replicas)
	for attempt := range attempts {
		resp, err := c.call(ctx, target, op, req, body)
		switch {
		case err != nil:
			lastErr = err
			// Re-applying a swap moves the record a second time and answers
			// with a different displaced generation, so the writer releases a
			// generation that is live. An attempt that never reached a replica
			// is safe to repeat; anything past that is in the log or not, and
			// the caller has to be told we cannot say which.
			if op == OpMetaSwap && !errors.Is(err, ErrUndelivered) {
				return nil, fmt.Errorf("swap %q: %w", req.Key, err)
			}
		case resp.Err == "":
			c.cacheLeader(target)
			return resp.Value, nil
		case resp.Err == ErrCodeInvalid:
			// The replica refused the request before proposing it, and every
			// replica would refuse it the same way.
			return nil, fmt.Errorf("write %q: replica %d: %s", req.Key, target, resp.Err)
		case resp.Err == ErrCodeNotLeader:
			lastErr = ErrNotLeader
			if id, perr := ParseRaftAddress(resp.Leader); perr == nil {
				// The replica knows the leader: go straight there.
				telemetry.RecordMetaRedirect(ctx, telemetry.RedirectNotLeader)
				target = id
				continue
			}
			// A replica that refuses and can name no leader means an election
			// is still settling, which is a different fault from being sent
			// somewhere else.
			telemetry.RecordMetaRedirect(ctx, telemetry.RedirectNoLeader)
		default:
			lastErr = fmt.Errorf("replica %d: %s", target, resp.Err)
			// A replica that answered with anything but a redirect may have
			// applied the command first: raft reports a lost leadership once
			// the entry is in the log, and the next leader can commit it.
			if op == OpMetaSwap {
				return nil, fmt.Errorf("swap %q: %w", req.Key, lastErr)
			}
		}

		// No redirect to follow — rotate to the next replica after a
		// short pause for elections or transient failures to resolve.
		next = (next + 1) % len(candidates)
		target = candidates[next]
		if attempt < attempts-1 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, fmt.Errorf("write %q: %w", req.Key, ctx.Err())
			case <-timer.C:
			}
		}
	}
	telemetry.RecordMetaRedirect(ctx, telemetry.RedirectRetryExhausted)
	return nil, fmt.Errorf("write %q failed after %d attempts: %w", req.Key, attempts, lastErr)
}
