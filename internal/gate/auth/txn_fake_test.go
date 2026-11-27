package auth_test

import (
	"context"

	"github.com/mulgadc/predastore/internal/meta"
)

func (noMeta) Txn(context.Context, []meta.TxnCheck, []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	return meta.TxnRefused, nil, errNoCluster
}
