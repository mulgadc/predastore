package s3

import (
	"testing"

	"github.com/mulgadc/predastore/s3db"
	"github.com/stretchr/testify/assert"
)

func TestAllHostsSame(t *testing.T) {
	tests := []struct {
		name  string
		nodes []Nodes
		want  bool
	}{
		{"empty", nil, true},
		{"single node", []Nodes{{Host: "localhost"}}, true},
		{"all same", []Nodes{{Host: "a"}, {Host: "a"}, {Host: "a"}}, true},
		{"different", []Nodes{{Host: "a"}, {Host: "b"}}, false},
		{"first differs", []Nodes{{Host: "x"}, {Host: "a"}, {Host: "a"}}, false},
		{"last differs", []Nodes{{Host: "a"}, {Host: "a"}, {Host: "b"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, allHostsSame(tt.nodes))
		})
	}
}

func TestAllDBHostsSame(t *testing.T) {
	tests := []struct {
		name  string
		nodes []s3db.DBNodeConfig
		want  bool
	}{
		{"empty", nil, true},
		{"single node", []s3db.DBNodeConfig{{Host: "localhost"}}, true},
		{"all same", []s3db.DBNodeConfig{{Host: "a"}, {Host: "a"}}, true},
		{"different", []s3db.DBNodeConfig{{Host: "a"}, {Host: "b"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, allDBHostsSame(tt.nodes))
		})
	}
}
