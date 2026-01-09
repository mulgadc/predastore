package s3db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Client provides access to the distributed database cluster
type Client struct {
	mu          sync.RWMutex
	nodes       []string // List of node addresses
	leaderAddr  string   // Cached leader address
	httpClient  *http.Client
	apiKey      string
	maxRetries  int
}

// ClientConfig holds client configuration
type ClientConfig struct {
	Nodes      []string      // Initial list of node addresses
	APIKey     string        // API key for authentication
	Timeout    time.Duration // HTTP request timeout
	MaxRetries int           // Max retries on failure
}

// DefaultClientConfig returns sensible defaults
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Timeout:    10 * time.Second,
		MaxRetries: 3,
	}
}

// NewClient creates a new database client
func NewClient(config *ClientConfig) *Client {
	if config == nil {
		config = DefaultClientConfig()
	}

	return &Client{
		nodes:      config.Nodes,
		apiKey:     config.APIKey,
		maxRetries: config.MaxRetries,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// Put stores a key-value pair in the specified table
func (c *Client) Put(table, key string, value []byte) error {
	path := fmt.Sprintf("/v1/put/%s/%s", url.PathEscape(table), url.PathEscape(key))
	_, err := c.doWrite("POST", path, value)
	return err
}

// Get retrieves a value by key from the specified table
func (c *Client) Get(table, key string) ([]byte, error) {
	path := fmt.Sprintf("/v1/get/%s/%s", url.PathEscape(table), url.PathEscape(key))

	resp, err := c.doRead(path)
	if err != nil {
		return nil, err
	}

	var result GetResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Value, nil
}

// Delete removes a key from the specified table
func (c *Client) Delete(table, key string) error {
	path := fmt.Sprintf("/v1/delete/%s/%s", url.PathEscape(table), url.PathEscape(key))
	_, err := c.doWrite("DELETE", path, nil)
	return err
}

// Scan lists keys with optional prefix in the specified table
func (c *Client) Scan(table, prefix string, limit int) ([]ScanItem, error) {
	path := fmt.Sprintf("/v1/scan/%s?prefix=%s&limit=%d",
		url.PathEscape(table),
		url.QueryEscape(prefix),
		limit)

	resp, err := c.doRead(path)
	if err != nil {
		return nil, err
	}

	var result ScanResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result.Items, nil
}

// Status returns the status of the connected node
func (c *Client) Status() (*StatusResponse, error) {
	resp, err := c.doRead("/status")
	if err != nil {
		return nil, err
	}

	var result StatusResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &result, nil
}

// Leader returns information about the current leader
func (c *Client) Leader() (string, string, error) {
	resp, err := c.doRead("/v1/leader")
	if err != nil {
		return "", "", err
	}

	var result struct {
		LeaderID   string `json:"leader_id"`
		LeaderAddr string `json:"leader_addr"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", "", fmt.Errorf("failed to parse response: %w", err)
	}

	return result.LeaderID, result.LeaderAddr, nil
}

// doRead performs a GET request, can go to any node
func (c *Client) doRead(path string) ([]byte, error) {
	c.mu.RLock()
	nodes := c.nodes
	c.mu.RUnlock()

	var lastErr error
	for i := 0; i < c.maxRetries; i++ {
		for _, node := range nodes {
			url := "http://" + node + path
			resp, err := c.httpClient.Get(url)
			if err != nil {
				lastErr = err
				continue
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				lastErr = err
				continue
			}

			if resp.StatusCode == http.StatusOK {
				return body, nil
			}

			// Parse error response
			var errResp ErrorResponse
			if json.Unmarshal(body, &errResp) == nil {
				if errResp.Error == "KeyNotFound" {
					return nil, ErrKeyNotFound
				}
				lastErr = fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
			} else {
				lastErr = fmt.Errorf("request failed with status %d", resp.StatusCode)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("all nodes failed: %w", lastErr)
}

// doWrite performs a write request, must go to leader
func (c *Client) doWrite(method, path string, body []byte) ([]byte, error) {
	c.mu.RLock()
	leaderAddr := c.leaderAddr
	nodes := c.nodes
	c.mu.RUnlock()

	// Try leader first if known
	if leaderAddr != "" {
		resp, newLeader, err := c.tryWrite(method, leaderAddr, path, body)
		if err == nil {
			return resp, nil
		}
		if newLeader != "" {
			c.mu.Lock()
			c.leaderAddr = newLeader
			c.mu.Unlock()
		}
	}

	// Try all nodes to find leader
	var lastErr error
	for i := 0; i < c.maxRetries; i++ {
		for _, node := range nodes {
			resp, newLeader, err := c.tryWrite(method, node, path, body)
			if err == nil {
				c.mu.Lock()
				c.leaderAddr = node
				c.mu.Unlock()
				return resp, nil
			}

			if newLeader != "" {
				// Got redirected to leader, try it
				resp, _, err = c.tryWrite(method, newLeader, path, body)
				if err == nil {
					c.mu.Lock()
					c.leaderAddr = newLeader
					c.mu.Unlock()
					return resp, nil
				}
			}
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("write failed: %w", lastErr)
}

// tryWrite attempts a write to a specific node
// Returns (response, leaderAddr, error)
func (c *Client) tryWrite(method, node, path string, body []byte) ([]byte, string, error) {
	url := "http://" + node + path

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewReader(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return nil, "", err
	}

	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	// Check for redirect to leader
	if resp.StatusCode == http.StatusTemporaryRedirect {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil {
			return nil, errResp.Leader, fmt.Errorf("not leader, redirect to %s", errResp.Leader)
		}
	}

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return respBody, "", nil
	}

	var errResp ErrorResponse
	if json.Unmarshal(respBody, &errResp) == nil {
		return nil, "", fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
	}

	return nil, "", fmt.Errorf("request failed with status %d", resp.StatusCode)
}

// AddNode adds a node to the client's node list
func (c *Client) AddNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	for _, n := range c.nodes {
		if n == addr {
			return
		}
	}
	c.nodes = append(c.nodes, addr)
}

// RemoveNode removes a node from the client's node list
func (c *Client) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, n := range c.nodes {
		if n == addr {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			return
		}
	}
}

// Errors
var (
	ErrKeyNotFound = fmt.Errorf("key not found")
)
