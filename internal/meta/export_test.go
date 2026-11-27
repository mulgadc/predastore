package meta

import (
	"context"

	"github.com/mulgadc/predastore/internal/config"
)

// ServeTxn registers the OpMetaTxn handler on a replica built from cfg, which
// no production configuration can do.
func ServeTxn(cfg *Config) { cfg.serveTxn = true }

// SendTxnBody sends body to target as an OpMetaTxn exactly as written, without
// the checks Txn makes first, so a test can show what the replica refuses.
func (c *Client) SendTxnBody(ctx context.Context, target config.NodeID, body []byte) (*MetaResponse, error) {
	return c.call(ctx, target, OpMetaTxn, &MetaRequest{}, body)
}
