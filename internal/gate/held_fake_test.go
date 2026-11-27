package gate

import (
	"context"
	"io"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
)

var _ handlers.HeldBlobClient = (*memBlob)(nil)

// OpenHeldPut registers a held write, as a node that supports Hold does.
// The prepared row it leaves commits and aborts like any other.
func (b *memBlob) OpenHeldPut(_ context.Context, node config.NodeID, req blob.PutRequest) (handlers.HeldPut, error) {
	return &memHeldPut{b: b, node: node, req: req}, nil
}

type memHeldPut struct {
	b    *memBlob
	node config.NodeID
	req  blob.PutRequest
}

func (p *memHeldPut) Send(ctx context.Context, body io.Reader) (*blob.PutResponse, error) {
	resp, err := p.b.Put(ctx, p.node, p.req, body)
	if err != nil {
		return nil, err
	}
	resp.Holding = true
	return resp, nil
}

func (p *memHeldPut) Abandon() {}
