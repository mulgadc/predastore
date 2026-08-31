package handlers

import (
	"context"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/chunked"
)

// SignedPayload is what authentication learned about a request body, carried to
// the write path because only the write path can act on it. The mode decides
// whether the body is framed at all, and the chain is what binds a framed body
// to the principal that signed the request.
//
// It carries the chain's inputs rather than the verified request itself: a
// handler needs to continue one chain, not the ability to re-sign anything.
type SignedPayload struct {
	// Signed distinguishes an authenticated request whose mode is empty — the
	// client signed a literal digest — from one that was never authenticated at
	// all. Both leave Mode empty and they are not the same request.
	Signed bool
	Mode   sigv4.ContentMode
	Chain  *chunked.Chain
}

// Framed reports whether the body carries aws-chunked framing. This is decided
// by the sentinel the client signed, never by Content-Encoding: that header is
// optional, may name other encodings alongside this one, and is not covered by
// the signature, so anything on the path can change it.
func (p SignedPayload) Framed() bool {
	switch p.Mode {
	case sigv4.StreamingSigned, sigv4.StreamingSignedTrailer, sigv4.StreamingUnsignedTrailer:
		return true
	default:
		return false
	}
}

// PromisesTrailer reports whether the sentinel the client signed names a
// trailing checksum. Both trailer modes commit to sending one, so a body that
// then omits it has not kept the promise its signature covers.
func (p SignedPayload) PromisesTrailer() bool {
	switch p.Mode {
	case sigv4.StreamingSignedTrailer, sigv4.StreamingUnsignedTrailer:
		return true
	default:
		return false
	}
}

type payloadContextKey struct{}

// WithSignedPayload attaches the payload classification to a request context.
func WithSignedPayload(ctx context.Context, p SignedPayload) context.Context {
	return context.WithValue(ctx, payloadContextKey{}, p)
}

// SignedPayloadFrom returns the classification, or the zero value on an
// unauthenticated request — a public-bucket write signs nothing, so there is no
// sentinel to trust and no chain to continue.
func SignedPayloadFrom(ctx context.Context) SignedPayload {
	p, _ := ctx.Value(payloadContextKey{}).(SignedPayload)
	return p
}
