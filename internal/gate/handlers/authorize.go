package handlers

import "context"

// ObjectAuthorizer reports whether the caller may take action on one key. The
// batch delete names its keys in the body, so only the handler can ask.
type ObjectAuthorizer func(action, key string) bool

type objectAuthorizerKey struct{}

// WithObjectAuthorizer attaches the per-key authorizer to a request context.
func WithObjectAuthorizer(ctx context.Context, authorize ObjectAuthorizer) context.Context {
	return context.WithValue(ctx, objectAuthorizerKey{}, authorize)
}

// objectAuthorizerFrom returns the request's authorizer, or one refusing every
// key: a request that reached a handler without one was never authorized.
func objectAuthorizerFrom(ctx context.Context) ObjectAuthorizer {
	if authorize, ok := ctx.Value(objectAuthorizerKey{}).(ObjectAuthorizer); ok && authorize != nil {
		return authorize
	}
	return func(string, string) bool { return false }
}
