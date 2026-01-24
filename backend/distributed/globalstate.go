package distributed

import (
	"github.com/mulgadc/predastore/s3db"
)

// GlobalState provides an abstraction for global state storage operations.
// This allows the distributed backend to use either a local BadgerDB
// or a distributed s3db cluster for storing object metadata.
type GlobalState interface {
	// Get retrieves a value by key from the specified table
	Get(table string, key []byte) ([]byte, error)

	// Set stores a key-value pair in the specified table
	Set(table string, key []byte, value []byte) error

	// Delete removes a key from the specified table
	Delete(table string, key []byte) error

	// Exists checks if a key exists in the specified table
	Exists(table string, key []byte) (bool, error)

	// ListKeys returns all keys with the given prefix in the specified table
	ListKeys(table string, prefix []byte) ([][]byte, error)

	// Scan iterates over all keys with the given prefix
	Scan(table string, prefix []byte, fn func(key, value []byte) error) error

	// Close closes the state store
	Close() error
}

// LocalState wraps a local s3db.S3DB for GlobalState operations.
// This is used when no distributed database is configured.
type LocalState struct {
	db *s3db.S3DB
}

// NewLocalState creates a GlobalState backed by a local BadgerDB
func NewLocalState(dbPath string) (*LocalState, error) {
	db, err := s3db.New(dbPath)
	if err != nil {
		return nil, err
	}
	return &LocalState{db: db}, nil
}

// Get retrieves a value by key (table is encoded in the key)
func (l *LocalState) Get(table string, key []byte) ([]byte, error) {
	fullKey := makeKey(table, key)
	return l.db.Get(fullKey)
}

// Set stores a key-value pair
func (l *LocalState) Set(table string, key []byte, value []byte) error {
	fullKey := makeKey(table, key)
	return l.db.Set(fullKey, value)
}

// Delete removes a key
func (l *LocalState) Delete(table string, key []byte) error {
	fullKey := makeKey(table, key)
	return l.db.Delete(fullKey)
}

// Exists checks if a key exists
func (l *LocalState) Exists(table string, key []byte) (bool, error) {
	fullKey := makeKey(table, key)
	return l.db.Exists(fullKey)
}

// ListKeys returns all keys with the given prefix
func (l *LocalState) ListKeys(table string, prefix []byte) ([][]byte, error) {
	fullPrefix := makeKey(table, prefix)
	keys, err := l.db.ListKeys(fullPrefix)
	if err != nil {
		return nil, err
	}

	// Strip the table prefix from keys
	tablePrefix := table + ":"
	result := make([][]byte, len(keys))
	for i, k := range keys {
		if len(k) > len(tablePrefix) {
			result[i] = k[len(tablePrefix):]
		} else {
			result[i] = k
		}
	}
	return result, nil
}

// Scan iterates over all keys with the given prefix
func (l *LocalState) Scan(table string, prefix []byte, fn func(key, value []byte) error) error {
	fullPrefix := makeKey(table, prefix)
	tablePrefix := table + ":"

	return l.db.Scan(fullPrefix, func(key, value []byte) error {
		// Strip table prefix from key before calling callback
		strippedKey := key
		if len(key) > len(tablePrefix) {
			strippedKey = key[len(tablePrefix):]
		}
		return fn(strippedKey, value)
	})
}

// Close closes the underlying database
func (l *LocalState) Close() error {
	return l.db.Close()
}

// DB returns the underlying S3DB for backward compatibility
func (l *LocalState) DB() *s3db.S3DB {
	return l.db
}

// DistributedState wraps an s3db.Client for GlobalState operations.
// This is used when a distributed database cluster is configured.
type DistributedState struct {
	client *s3db.Client
}

// DBClientConfig holds configuration for connecting to the distributed database
type DBClientConfig struct {
	Nodes           []string // List of DB node addresses
	AccessKeyID     string   // AWS-style access key ID
	SecretAccessKey string   // AWS-style secret access key
	Region          string   // Region for signing (default: us-east-1)
}

// NewDistributedState creates a GlobalState backed by a distributed s3db cluster
func NewDistributedState(cfg *DBClientConfig) (*DistributedState, error) {
	if cfg == nil || len(cfg.Nodes) == 0 {
		return nil, nil
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	client := s3db.NewClient(&s3db.ClientConfig{
		Nodes:              cfg.Nodes,
		AccessKeyID:        cfg.AccessKeyID,
		SecretAccessKey:    cfg.SecretAccessKey,
		Region:             region,
		Service:            "s3db",
		InsecureSkipVerify: true, // For self-signed certs
		MaxRetries:         3,    // Required - 0 would skip the retry loop entirely
	})

	return &DistributedState{client: client}, nil
}

// Get retrieves a value by key from the distributed database
func (d *DistributedState) Get(table string, key []byte) ([]byte, error) {
	return d.client.Get(table, string(key))
}

// Set stores a key-value pair in the distributed database
func (d *DistributedState) Set(table string, key []byte, value []byte) error {
	return d.client.Put(table, string(key), value)
}

// Delete removes a key from the distributed database
func (d *DistributedState) Delete(table string, key []byte) error {
	return d.client.Delete(table, string(key))
}

// Exists checks if a key exists in the distributed database
func (d *DistributedState) Exists(table string, key []byte) (bool, error) {
	_, err := d.client.Get(table, string(key))
	if err == s3db.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListKeys returns all keys with the given prefix (via Scan)
func (d *DistributedState) ListKeys(table string, prefix []byte) ([][]byte, error) {
	items, err := d.client.Scan(table, string(prefix), 10000) // Large limit
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, len(items))
	for i, item := range items {
		keys[i] = []byte(item.Key)
	}
	return keys, nil
}

// Scan iterates over all keys with the given prefix
func (d *DistributedState) Scan(table string, prefix []byte, fn func(key, value []byte) error) error {
	items, err := d.client.Scan(table, string(prefix), 10000)
	if err != nil {
		return err
	}

	for _, item := range items {
		if err := fn([]byte(item.Key), item.Value); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for the distributed client (connections are reused)
func (d *DistributedState) Close() error {
	return nil
}

// Client returns the underlying s3db.Client
func (d *DistributedState) Client() *s3db.Client {
	return d.client
}

// makeKey creates a full key by prepending the table name
func makeKey(table string, key []byte) []byte {
	return append([]byte(table+":"), key...)
}

// Table names for global state
const (
	TableObjects   = "objects"   // Object metadata (hash -> shard locations)
	TableBuckets   = "buckets"   // Bucket metadata
	TableMultipart = "multipart" // Multipart upload metadata (uploadID -> metadata)
	TableParts     = "parts"     // Part metadata (uploadID:partNumber -> part info)
)
