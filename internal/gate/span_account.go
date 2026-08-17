package gate

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// attrAccountID names the account a span was served for. It matches the key
// the AWS gateway and the NATS helpers use, so one field attributes a request
// across every service that touches it.
const attrAccountID = "aws.account_id"

// annotateSpanAccount records accountID on the request's transaction span. An
// empty account is left off rather than recorded as blank: a request that never
// authenticated belongs to nobody, and a blank value reads as a tenant whose id
// went missing.
func annotateSpanAccount(ctx context.Context, accountID string) {
	if accountID == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String(attrAccountID, accountID))
}
