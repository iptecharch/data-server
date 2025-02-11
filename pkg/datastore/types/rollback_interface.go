package types

import (
	"context"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
)

type RollbackInterface interface {
	TransactionRollback(ctx context.Context, transaction *Transaction, dryRun bool) (*sdcpb.TransactionSetResponse, error)
}
