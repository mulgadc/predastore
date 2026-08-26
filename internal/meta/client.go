package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
)

// ErrNotFound is returned by reads when no replica holds the key.
var ErrNotFound = errors.New("key not found")

// ErrNotLeader is returned by a Server write this replica cannot commit, and
// by client writes no replica would accept as leader, which usually means an
// election is still settling.
var ErrNotLeader = errors.New("meta replica is not the leader")

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
// redirects and cache the leader they land on.
type Client struct {
	rpc        *rpc.Endpoint
	replicas   []config.NodeID
	timeout    time.Duration
	maxRetries int

	mu     sync.Mutex
	leader config.NodeID // cached leader replica id; 0 means unknown
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// Client carries the streams; it owns the mapping from node id to
	// address, so this client only ever names replicas by id.
	Client *rpc.Endpoint
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
		rpc:        cfg.Client,
		replicas:   cfg.Replicas,
		timeout:    cfg.Timeout,
		maxRetries: cfg.MaxRetries,
	}, nil
}

// call performs one request round trip against a replica: header, optional
// body, half-close, then the response envelope. The configured timeout bounds
// the attempt only as a fallback; cancelling ctx aborts it sooner.
func (c *Client) call(ctx context.Context, target config.NodeID, op rpc.Opcode, req *MetaRequest, body []byte) (*MetaResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	stream, err := rpc.OpenStream(ctx, c.rpc, target, op, req)
	if err != nil {
		return nil, fmt.Errorf("open stream to replica %d: %w", target, err)
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

	var resp MetaResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("decode response from replica %d: %w", target, err)
	}
	return &resp, nil
}

// readOrder returns replicas with the cached leader first.
func (c *Client) readOrder() []config.NodeID {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	order := make([]config.NodeID, 0, len(c.replicas))
	if leader != 0 {
		order = append(order, leader)
	}
	for _, id := range c.replicas {
		if id != leader {
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

// request builds a wire header for a key. Callers hand keys over as strings;
// the wire carries them as bytes so binary keys survive JSON.
func request(key string, limit int) *MetaRequest {
	return &MetaRequest{Key: []byte(key), Limit: limit}
}

// Get retrieves a value. A replica that has not applied the key yet answers
// not-found, so every replica is consulted before giving up.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
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
	var lastErr error
	for _, id := range c.readOrder() {
		resp, err := c.call(ctx, id, OpMetaScan, request(prefix, limit), nil)
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
func (c *Client) Status(ctx context.Context, target config.NodeID) (MetaStatus, error) {
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

	var status MetaStatus
	if err := json.NewDecoder(stream).Decode(&status); err != nil {
		stream.CancelRead(0)
		return MetaStatus{}, fmt.Errorf("decode status from replica %d: %w", target, err)
	}
	return status, nil
}

// Put stores a key-value pair through the leader.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.write(ctx, OpMetaPut, request(key, 0), value)
}

// Delete removes a key through the leader.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.write(ctx, OpMetaDelete, request(key, 0), nil)
}

// write drives a consensus write to the leader, following not-leader
// redirects and rotating through replicas while an election settles.
func (c *Client) write(ctx context.Context, op rpc.Opcode, req *MetaRequest, body []byte) error {
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
		case resp.Err == "":
			c.cacheLeader(target)
			return nil
		case resp.Err == ErrCodeNotLeader:
			lastErr = ErrNotLeader
			if id, perr := ParseRaftAddress(resp.Leader); perr == nil {
				// The replica knows the leader: go straight there.
				target = id
				continue
			}
		default:
			lastErr = fmt.Errorf("replica %d: %s", target, resp.Err)
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
				return fmt.Errorf("write %q: %w", req.Key, ctx.Err())
			case <-timer.C:
			}
		}
	}
	return fmt.Errorf("write %q failed after %d attempts: %w", req.Key, attempts, lastErr)
}
