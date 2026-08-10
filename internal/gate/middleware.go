package gate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/pkg/iampolicy"
	"github.com/mulgadc/predastore/pkg/otelsetup"
	"github.com/mulgadc/predastore/pkg/ratelimit"
	"github.com/mulgadc/predastore/pkg/sigv4"
)

// globalSigningRegion is the region clients sign S3's global operations against,
// independent of the region they are configured for or the endpoint they target.
const globalSigningRegion = "us-east-1"

// setupMiddleware installs the chain every request passes through before it
// reaches a handler. chi requires all middleware to be registered before the
// first route, so this runs ahead of setupRoutes.
func (s *Server) setupMiddleware() {
	r := s.router

	r.Use(otelsetup.HTTPMiddleware("predastore"))
	r.Use(s3SpanMiddleware)
	// chi's access log duplicates the APM transaction from HTTPMiddleware and
	// is a synchronous per-request write on the hot path; only enable it for
	// explicit debug sessions.
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		r.Use(middleware.Logger)
	}
	r.Use(middleware.Recoverer)
	// AWS S3 accepts bucket-scoped URLs with or without a trailing slash
	// (e.g. PUT /bucket/ == PUT /bucket for CreateBucket) without redirecting.
	// StripSlashes only rewrites chi's routing context, not r.URL.Path, so
	// SigV4 verification still sees the exact URI the client signed.
	r.Use(middleware.StripSlashes)
	r.Use(s.sigV4AuthMiddleware)

	// API request throttling (post-auth, per-account + per-action)
	if s.throttler != nil {
		r.Use(s.throttler.Middleware(
			[]ratelimit.KeyFunc{
				func(r *http.Request) (string, error) {
					acct := auth.AccountID(r.Context())
					if acct == "" {
						return "", fmt.Errorf("account-id missing from request context")
					}
					return acct, nil
				},
				func(r *http.Request) (string, error) {
					return s3Action(r.Method, r.URL.Path), nil
				},
			},
			func(w http.ResponseWriter, r *http.Request) {
				handlers.WriteS3Error(w, r, http.StatusServiceUnavailable, "SlowDown",
					"Please reduce your request rate.")
			},
		))
	}
}

// sigV4AuthMiddleware authenticates and authorizes incoming S3 requests:
// public-bucket short-circuit, SigV4 verify, IAM policy eval, and
// cross-account bucket-ownership check.
func (s *Server) sigV4AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method

		publicBucketAccess := s.config.validatePublicBucketPermission(method, path)

		// Parse recognizes both header-authed and presigned requests; only a request with
		// neither returns ErrMissingAuthentication.
		sig, err := sigv4.Parse(r)
		if err != nil {
			// Unsigned: fall back to public-bucket access rather than a parse error.
			if errors.Is(err, sigv4.ErrMissingAuthentication) {
				if publicBucketAccess == nil {
					next.ServeHTTP(w, r)
					return
				}
				handlers.WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
			handlers.RespondSigV4Error(w, r, "", err, nil)
			return
		}

		accessKey := sig.Credential.AccessKeyID
		credResult, err := s.credProv.LookupCredentials(accessKey)
		if err != nil {
			handlers.RespondSigV4Error(w, r, accessKey, nil, err)
			return
		}

		// Regional operations must carry the endpoint's own region, as AWS enforces.
		expectedRegion := s.config.Region

		// ListBuckets is the only global operation served here, and clients sign it against
		// us-east-1 whatever region they are configured for. Some SDKs sign it with the
		// configured region instead, so accept either rather than pinning to us-east-1.
		if bucket, _ := parseS3Path(path); bucket == "" && sig.Credential.Region == globalSigningRegion {
			expectedRegion = globalSigningRegion
		}

		if _, err := sig.Verify(credResult.SecretAccessKey, expectedRegion, "s3"); err != nil {
			handlers.RespondSigV4Error(w, r, accessKey, err, nil)
			return
		}

		// IAM policy evaluation (NATS-sourced credentials only).
		if !credResult.SkipPolicyCheck {
			action := s3Action(method, path)
			if action == "" {
				slog.WarnContext(r.Context(), "Unsupported HTTP method for S3 action mapping",
					"method", method, "path", path, "remoteAddr", r.RemoteAddr)
				handlers.WriteS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed")
				return
			}
			resource := s3Resource(path)
			if len(credResult.PolicyDocuments) == 0 {
				slog.DebugContext(r.Context(), "No policies resolved for user, implicit deny",
					"accessKeyID", accessKey, "accountID", credResult.AccountID)
			}
			if iampolicy.Evaluate(action, resource, credResult.PolicyDocuments) == iampolicy.Deny {
				slog.DebugContext(r.Context(), "S3 access denied by policy",
					"action", action, "resource", resource,
					"accessKeyID", accessKey, "policyCount", len(credResult.PolicyDocuments))
				handlers.WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
		}

		// Bucket-ownership check (default-deny on cross-account access).
		// Runs after IAM evaluation so explicit IAM denies still short-circuit.
		// Skipped for ListAllMyBuckets (no bucket component, already account-scoped)
		// and CreateBucket (no existing owner). A sub-resource query on a bare
		// bucket (?policy, ?acl, ?versioning, ...) is NOT CreateBucket and must
		// stay subject to the cross-account check.
		bucket, key := parseS3Path(path)
		isCreateBucket := method == http.MethodPut && bucket != "" && key == "" && r.URL.RawQuery == ""
		if bucket != "" && !isCreateBucket {
			meta, err := handlers.ResolveBucketMetadata(s.state, s.handlerCfg, bucket)
			if err != nil {
				slog.ErrorContext(r.Context(), "Failed to resolve bucket metadata for ownership check",
					"bucket", bucket, "error", err, "accessKeyID", accessKey)
				handlers.WriteS3Error(w, r, http.StatusInternalServerError, "InternalError",
					"An internal error occurred")
				return
			}
			// Unknown bucket — let the route handler return NoSuchBucket so
			// existence is reported consistently with non-authenticated paths.
			if meta != nil && !bucketAccessAllowed(method, credResult.AccountID, meta, credResult.SkipPolicyCheck) {
				slog.WarnContext(r.Context(), "Cross-account bucket access denied",
					"accessKeyID", accessKey,
					"callerAccountID", credResult.AccountID,
					"bucketAccountID", meta.AccountID,
					"bucket", bucket,
					"action", s3Action(method, path),
					"resource", s3Resource(path))
				handlers.WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
		}

		ctx := context.WithValue(r.Context(), auth.ContextKeyAccessKeyID, accessKey)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, credResult.AccountID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
