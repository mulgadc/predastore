package backend

import (
	"fmt"
	"sync"

	"github.com/gofiber/fiber/v2"
)

// Backend interface defines all storage backend operations
type Backend interface {
	// Object operations
	DeleteObject(bucket string, file string, c *fiber.Ctx) error
	GetObject(bucket string, file string, c *fiber.Ctx) error
	GetObjectHead(bucket string, file string, c *fiber.Ctx) error
	PutObject(bucket string, file string, c *fiber.Ctx) error

	// Bucket operations
	ListBuckets(c *fiber.Ctx) error
	ListObjectsV2Handler(bucket string, c *fiber.Ctx) error

	// Multipart upload operations
	CreateMultipartUpload(bucket string, file string, c *fiber.Ctx) error
	CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) error
	PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx) error
}

// Factory is a function that creates a new Backend instance
type Factory func(config interface{}) (Backend, error)

var (
	factoryMu  sync.RWMutex
	factories  = make(map[string]Factory)
)

// Register registers a backend factory under the given name.
// This is typically called from init() functions in backend packages.
func Register(name string, factory Factory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	factories[name] = factory
}

// New creates a new backend instance by name using the registered factory.
func New(name string, config interface{}) (Backend, error) {
	factoryMu.RLock()
	factory, ok := factories[name]
	factoryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown backend type: %s", name)
	}

	return factory(config)
}

// Available returns a list of registered backend names.
func Available() []string {
	factoryMu.RLock()
	defer factoryMu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}
