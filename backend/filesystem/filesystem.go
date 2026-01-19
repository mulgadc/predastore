package filesystem

import (
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/s3"
)

func init() {
	backend.Register("filesystem", func(config interface{}) (backend.Backend, error) {
		return New(config)
	})
}

// Backend implements the filesystem storage backend
type Backend struct {
	Config *s3.Config
}

// New creates a new filesystem backend
func New(config interface{}) (*Backend, error) {
	s3Config, ok := config.(*s3.Config)
	if !ok {
		// If not a pointer, try direct type assertion
		if cfg, ok := config.(s3.Config); ok {
			s3Config = &cfg
		} else {
			return nil, nil
		}
	}

	return &Backend{
		Config: s3Config,
	}, nil
}
