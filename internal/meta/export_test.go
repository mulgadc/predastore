package meta

import (
	"context"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
)

// SendTxnBody sends body to target as an OpMetaTxn exactly as written, without
// the checks Txn makes first, so a test can show what the replica refuses.
func (c *Client) SendTxnBody(ctx context.Context, target config.NodeID, body []byte) (*MetaResponse, error) {
	return c.call(ctx, target, OpMetaTxn, &MetaRequest{}, body)
}

// SendWrite sends one keyed write to target without the client's own checks,
// so a test can show what the replica refuses.
func (c *Client) SendWrite(ctx context.Context, target config.NodeID, op rpc.Opcode, key string, value []byte) (*MetaResponse, error) {
	return c.call(ctx, target, op, request(key, 0), value)
}

// encodeApplied returns the applied row value for index.
func encodeApplied(index uint64) []byte {
	v := make([]byte, appliedValueLen)
	putApplied(v, index)
	return v
}
