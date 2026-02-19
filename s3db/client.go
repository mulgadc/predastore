package s3db

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// Client provides access to the distributed database cluster
type Client struct {
	mu              sync.RWMutex
	nodes           []string // List of node addresses
	leaderAddr      string   // Cached leader address
	httpClient      *http.Client
	accessKeyID     string
	secretAccessKey string
	region          string
	service         string
	maxRetries      int
}

// ClientConfig holds client configuration
type ClientConfig struct {
	Nodes              []string      // Initial list of node addresses
	AccessKeyID        string        // AWS-style access key ID
	SecretAccessKey    string        // AWS-style secret access key
	Region             string        // Region for signing (default: us-east-1)
	Service            string        // Service name for signing (default: s3db)
	Timeout            time.Duration // HTTP request timeout
	MaxRetries         int           // Max retries on failure
	InsecureSkipVerify bool          // Skip TLS certificate verification (for self-signed certs)
}

// DefaultClientConfig returns sensible defaults
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Region:             DefaultRegion,
		Service:            DefaultService,
		Timeout:            10 * time.Second,
		MaxRetries:         3,
		InsecureSkipVerify: true, // Default to true for self-signed certs
	}
}

// NewClient creates a new database client
func NewClient(config *ClientConfig) *Client {
	if config == nil {
		config = DefaultClientConfig()
	}

	region := config.Region
	if region == "" {
		region = DefaultRegion
	}

	service := config.Service
	if service == "" {
		service = DefaultService
	}

	// Configure TLS transport with optional certificate verification skip
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
			NextProtos:         []string{"h2", "http/1.1"},
		},
		ForceAttemptHTTP2: true,
	}

	// Configure HTTP/2 support with custom TLS config
	if err := http2.ConfigureTransport(transport); err != nil {
		slog.Error("failed to configure HTTP/2 transport, falling back to HTTP/1.1", "error", err)
	}

	return &Client{
		nodes:           config.Nodes,
		accessKeyID:     config.AccessKeyID,
		secretAccessKey: config.SecretAccessKey,
		region:          region,
		service:         service,
		maxRetries:      config.MaxRetries,
		httpClient: &http.Client{
			Timeout:   config.Timeout,
			Transport: transport,
		},
	}
}

// escapePathSegment escapes a path segment, ensuring all special characters
// (including +) are percent-encoded. This is needed for consistent signature
// verification since our UriEncode function encodes + to %2B.
func escapePathSegment(s string) string {
	// url.PathEscape doesn't encode +, but we need it encoded for signatures
	escaped := url.PathEscape(s)
	// Replace any remaining + with %2B
	return strings.Replace(escaped, "+", "%2B", -1)
}

// hexEncodeKey hex-encodes a key for safe transport in URL paths.
// This avoids issues with binary keys containing characters like '/' that
// can cause path normalization differences between Go's URL parser and fasthttp.
func hexEncodeKey(key string) string {
	return hex.EncodeToString([]byte(key))
}

// Put stores a key-value pair in the specified table
func (c *Client) Put(table, key string, value []byte) error {
	// Hex-encode the key to avoid URL path issues with binary data
	path := fmt.Sprintf("/v1/put/%s/%s", escapePathSegment(table), hexEncodeKey(key))
	_, err := c.doWrite("POST", path, value)
	return err
}

// Get retrieves a value by key from the specified table
func (c *Client) Get(table, key string) ([]byte, error) {
	// Hex-encode the key to avoid URL path issues with binary data
	path := fmt.Sprintf("/v1/get/%s/%s", escapePathSegment(table), hexEncodeKey(key))

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
	// Hex-encode the key to avoid URL path issues with binary data
	path := fmt.Sprintf("/v1/delete/%s/%s", escapePathSegment(table), hexEncodeKey(key))
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

// doRead performs a GET request, tries leader first then all other nodes
func (c *Client) doRead(path string) ([]byte, error) {
	c.mu.RLock()
	leaderAddr := c.leaderAddr
	nodes := c.nodes
	c.mu.RUnlock()

	// Build node list with leader first for read-your-writes consistency
	orderedNodes := make([]string, 0, len(nodes)+1)
	if leaderAddr != "" {
		orderedNodes = append(orderedNodes, leaderAddr)
	}
	for _, node := range nodes {
		if node != leaderAddr {
			orderedNodes = append(orderedNodes, node)
		}
	}

	var lastErr error
	keyNotFoundCount := 0
	for i := 0; i < c.maxRetries; i++ {
		for _, node := range orderedNodes {
			reqURL := "https://" + node + path
			req, err := http.NewRequest("GET", reqURL, nil)
			if err != nil {
				lastErr = err
				continue
			}

			// Sign the request if credentials are configured
			if c.accessKeyID != "" && c.secretAccessKey != "" {
				if err := SignRequest(req, c.accessKeyID, c.secretAccessKey, c.region, c.service); err != nil {
					lastErr = fmt.Errorf("failed to sign request: %w", err)
					continue
				}
			}

			resp, err := c.httpClient.Do(req)
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
					// Continue to try other nodes - data might not be replicated yet
					keyNotFoundCount++
					lastErr = ErrKeyNotFound
					continue
				}
				lastErr = fmt.Errorf("%s: %s", errResp.Error, errResp.Message)
			} else {
				lastErr = fmt.Errorf("request failed with status %d", resp.StatusCode)
			}
		}
		// If all nodes returned KeyNotFound, the key truly doesn't exist
		if keyNotFoundCount == len(orderedNodes) {
			return nil, ErrKeyNotFound
		}
		keyNotFoundCount = 0
		time.Sleep(100 * time.Millisecond)
	}

	// If we exhausted retries and the last error was KeyNotFound, return that
	if lastErr == ErrKeyNotFound {
		return nil, ErrKeyNotFound
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
	reqURL := "https://" + node + path

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, reqURL, bytes.NewReader(body))
	} else {
		req, err = http.NewRequest(method, reqURL, nil)
	}
	if err != nil {
		return nil, "", err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	// Sign the request if credentials are configured
	if c.accessKeyID != "" && c.secretAccessKey != "" {
		if err := SignRequest(req, c.accessKeyID, c.secretAccessKey, c.region, c.service); err != nil {
			return nil, "", fmt.Errorf("failed to sign request: %w", err)
		}
	}

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
	if slices.Contains(c.nodes, addr) {
		return
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
