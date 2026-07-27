package s3db

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/wire"
)

// RPCClient reads and writes global state over rpc streams. It mirrors the
// HTTP client's semantics: reads try the cached leader then every replica,
// writes follow not-leader redirects and cache the leader they land on.
type RPCClient struct {
	rpc        *rpc.Client
	resolve    func(nodeID uint64) (net.Addr, error)
	replicas   []uint64
	timeout    time.Duration
	maxRetries int

	mu     sync.Mutex
	leader uint64 // cached leader replica id; 0 means unknown
}

// RPCClientConfig configures an RPCClient.
type RPCClientConfig struct {
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

func NewRPCClient(cfg RPCClientConfig) (*RPCClient, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("s3db rpc client: missing rpc client")
	}
	if cfg.Resolve == nil {
		return nil, fmt.Errorf("s3db rpc client: missing resolver")
	}
	if len(cfg.Replicas) == 0 {
		return nil, fmt.Errorf("s3db rpc client: no state replicas")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	return &RPCClient{
		rpc:        cfg.Client,
		resolve:    cfg.Resolve,
		replicas:   cfg.Replicas,
		timeout:    cfg.Timeout,
		maxRetries: cfg.MaxRetries,
	}, nil
}

// call performs one request round trip against a replica: header, optional
// body, half-close, then the response envelope.
func (c *RPCClient) call(target uint64, op rpc.Opcode, req *wire.StateRequest, body []byte) (*wire.StateResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	addr, err := c.resolve(target)
	if err != nil {
		return nil, fmt.Errorf("resolve replica %d: %w", target, err)
	}
	req.Target = target
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

	var resp wire.StateResponse
	if err := json.NewDecoder(stream).Decode(&resp); err != nil {
		stream.CancelRead(0)
		return nil, fmt.Errorf("decode response from replica %d: %w", target, err)
	}
	return &resp, nil
}

// readOrder returns replicas with the cached leader first.
func (c *RPCClient) readOrder() []uint64 {
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

func (c *RPCClient) cacheLeader(id uint64) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

// Get retrieves a value. A replica that has not applied the key yet answers
// not-found, so every replica is consulted before giving up.
func (c *RPCClient) Get(table, key string) ([]byte, error) {
	var lastErr error
	notFound := false
	for _, id := range c.readOrder() {
		resp, err := c.call(id, wire.OpStateGet, &wire.StateRequest{Table: table, Key: key}, nil)
		if err != nil {
			lastErr = err
			continue
		}
		switch resp.Err {
		case "":
			return resp.Value, nil
		case wire.ErrCodeNotFound:
			notFound = true
		default:
			lastErr = fmt.Errorf("replica %d: %s", id, resp.Err)
		}
	}
	if notFound {
		return nil, ErrKeyNotFound
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("get %s/%s: no replica answered", table, key)
}

// Scan lists up to limit keys with the prefix, preferring the leader for
// freshness but accepting any replica.
func (c *RPCClient) Scan(table, prefix string, limit int) ([]ScanItem, error) {
	var lastErr error
	for _, id := range c.readOrder() {
		resp, err := c.call(id, wire.OpStateScan, &wire.StateRequest{Table: table, Key: prefix, Limit: limit}, nil)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Err != "" {
			lastErr = fmt.Errorf("replica %d: %s", id, resp.Err)
			continue
		}
		items := make([]ScanItem, len(resp.Items))
		for i, it := range resp.Items {
			items[i] = ScanItem{Key: it.Key, Value: it.Value}
		}
		return items, nil
	}
	return nil, lastErr
}

// Put stores a key-value pair through the leader.
func (c *RPCClient) Put(table, key string, value []byte) error {
	return c.write(wire.OpStatePut, &wire.StateRequest{Table: table, Key: key}, value)
}

// Delete removes a key through the leader.
func (c *RPCClient) Delete(table, key string) error {
	return c.write(wire.OpStateDelete, &wire.StateRequest{Table: table, Key: key}, nil)
}

// write drives a consensus write to the leader, following not-leader
// redirects and rotating through replicas while an election settles.
func (c *RPCClient) write(op rpc.Opcode, req *wire.StateRequest, body []byte) error {
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
		case resp.Err == wire.ErrCodeNotLeader:
			lastErr = ErrNotLeader
			if id, perr := wire.ParseRaftAddress(resp.Leader); perr == nil {
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
	return fmt.Errorf("write %s/%s failed after %d attempts: %w", req.Table, req.Key, attempts, lastErr)
}
