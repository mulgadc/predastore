package backend

import (
	"context"
	"fmt"
	"sync"
)

// Backend defines the interface for storage backends.
// All methods accept context.Context for cancellation and timeouts.
// This interface is HTTP-layer agnostic - no framework-specific types.
type Backend interface {
	// Object operations
	GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResponse, error)
	HeadObject(ctx context.Context, bucket, key string) (*HeadObjectResponse, error)
	PutObject(ctx context.Context, req *PutObjectRequest) (*PutObjectResponse, error)
	DeleteObject(ctx context.Context, req *DeleteObjectRequest) error

	// Bucket operations
	ListBuckets(ctx context.Context) (*ListBucketsResponse, error)
	ListObjects(ctx context.Context, req *ListObjectsRequest) (*ListObjectsResponse, error)

	// Multipart upload operations
	CreateMultipartUpload(ctx context.Context, req *CreateMultipartUploadRequest) (*CreateMultipartUploadResponse, error)
	UploadPart(ctx context.Context, req *UploadPartRequest) (*UploadPartResponse, error)
	CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResponse, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// Backend info
	Type() string
	Close() error
}

// BackendFactory creates a new backend instance
type BackendFactory func(config interface{}) (Backend, error)

// Registry holds registered backend factories
type Registry struct {
	mu        sync.RWMutex
	factories map[string]BackendFactory
}

// NewRegistry creates a new backend registry
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]BackendFactory),
	}
}

// Register adds a backend factory to the registry
func (r *Registry) Register(name string, factory BackendFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// Create instantiates a backend by name
func (r *Registry) Create(name string, config interface{}) (Backend, error) {
	r.mu.RLock()
	factory, exists := r.factories[name]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown backend type: %s", name)
	}

	return factory(config)
}

// DefaultRegistry is the global backend registry
var DefaultRegistry = NewRegistry()

// Register adds a backend factory to the default registry
func Register(name string, factory BackendFactory) {
	DefaultRegistry.Register(name, factory)
}

// Create instantiates a backend from the default registry
func Create(name string, config interface{}) (Backend, error) {
	return DefaultRegistry.Create(name, config)
}
