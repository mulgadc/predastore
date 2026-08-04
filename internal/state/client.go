package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/rpc"
)

// ErrNotFound is returned by reads when no replica holds the key.
var ErrNotFound = errors.New("state key not found")

// ErrNotLeader is returned by writes no replica would accept as leader, which
// usually means an election is still settling.
var ErrNotLeader = errors.New("state replica is not the leader")

// Item is one key-value pair returned by a scan.
type Item struct {
	Key   string
	Value []byte
}

// Client reads and writes global state over rpc streams, hiding the wire
// protocol from callers: it owns table scoping and key encoding, and takes
// keys as strings that may hold arbitrary bytes.
//
// Reads try the cached leader then every replica; writes follow not-leader
// redirects and cache the leader they land on.
type Client struct {
	rpc        *rpc.Client
	resolve    func(nodeID uint64) (net.Addr, error)
	replicas   []uint64
	timeout    time.Duration
	maxRetries int

	mu     sync.Mutex
	leader uint64 // cached leader replica id; 0 means unknown
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// Client carries the streams; its transports decide pipe vs network
	// per address.
	Client *rpc.Client
	// Resolve maps a replica node id to the address to dial.
	Resolve func(nodeID uint64) (net.Addr, error)
	// Replicas lists the state replica node ids.
	Replicas []uint64
	// Timeout bounds each attempt. Default 10s.
	Timeout time.Duration
	// MaxRetries bounds write retry rounds across the replica set.
	// Default 3.
	MaxRetries int
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("state client: missing rpc client")
	}
	if cfg.Resolve == nil {
		return nil, fmt.Errorf("state client: missing resolver")
	}
	if len(cfg.Replicas) == 0 {
		return nil, fmt.Errorf("state client: no state replicas")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	return &Client{
		rpc:        cfg.Client,
		resolve:    cfg.Resolve,
		replicas:   cfg.Replicas,
		timeout:    cfg.Timeout,
		maxRetries: cfg.MaxRetries,
	}, nil
}

// call performs one request round trip against a replica: header, optional
// body, half-close, then the response envelope.
func (c *Client) call(target uint64, op rpc.Opcode, req *StateRequest, body []byte) (*StateResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	addr, err := c.resolve(target)
	if err != nil {
		return nil, fmt.Errorf("resolve replica %d: %w", target, err)
	}
	stream, err := rpc.OpenStream(ctx, c.rpc, addr, op, req)
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

	var resp StateResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("decode response from replica %d: %w", target, err)
	}
	return &resp, nil
}

// readOrder returns replicas with the cached leader first.
func (c *Client) readOrder() []uint64 {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	order := make([]uint64, 0, len(c.replicas))
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

func (c *Client) cacheLeader(id uint64) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

// request builds a wire header for a table-scoped key. Callers hand keys over
// as strings; the wire carries them as bytes so binary keys survive JSON.
func request(table, key string, limit int) *StateRequest {
	return &StateRequest{Table: table, Key: []byte(key), Limit: limit}
}

// Get retrieves a value. A replica that has not applied the key yet answers
// not-found, so every replica is consulted before giving up.
func (c *Client) Get(table, key string) ([]byte, error) {
	var lastErr error
	notFound := false
	for _, id := range c.readOrder() {
		resp, err := c.call(id, OpStateGet, request(table, key, 0), nil)
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
		return nil, fmt.Errorf("get %s/%q: %w", table, key, ErrNotFound)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("get %s/%q: no replica answered", table, key)
}

// Exists reports whether the key is present.
func (c *Client) Exists(table, key string) (bool, error) {
	_, err := c.Get(table, key)
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
// returns every match.
func (c *Client) Scan(table, prefix string, limit int) ([]Item, error) {
	var lastErr error
	for _, id := range c.readOrder() {
		resp, err := c.call(id, OpStateScan, request(table, prefix, limit), nil)
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
func (c *Client) ListKeys(table, prefix string) ([]string, error) {
	items, err := c.Scan(table, prefix, 0)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = item.Key
	}
	return keys, nil
}

// Put stores a key-value pair through the leader.
func (c *Client) Put(table, key string, value []byte) error {
	return c.write(OpStatePut, request(table, key, 0), value)
}

// Delete removes a key through the leader.
func (c *Client) Delete(table, key string) error {
	return c.write(OpStateDelete, request(table, key, 0), nil)
}

// write drives a consensus write to the leader, following not-leader
// redirects and rotating through replicas while an election settles.
func (c *Client) write(op rpc.Opcode, req *StateRequest, body []byte) error {
	candidates := c.readOrder()
	next := 0
	target := candidates[next]

	var lastErr error
	attempts := c.maxRetries * len(c.replicas)
	for attempt := range attempts {
		resp, err := c.call(target, op, req, body)
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
			time.Sleep(100 * time.Millisecond)
		}
	}
	return fmt.Errorf("write %s/%q failed after %d attempts: %w", req.Table, req.Key, attempts, lastErr)
}
