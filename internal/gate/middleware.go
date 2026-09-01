package gate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/bluebottle/pkg/otelsetup"
	"github.com/mulgadc/bluebottle/pkg/ratelimit"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/handlers"
)

// globalSigningRegion is the region clients sign S3's global operations against,
// independent of the region they are configured for or the endpoint they target.
const globalSigningRegion = "us-east-1"

// requestDeadline bounds the work one request may do, so the gate answers with
// an error naming the fault instead of the connection being reset with no
// explanation logged on either side.
//
// It is a bound on the fixed exchanges, not on object data. A body is bounded
// by progress instead: see bulkBody.
const requestDeadline = 50 * time.Second

type deadlineStopperKey struct{}

// deadlineStopper releases the request deadline. It is carried in the context
// rather than applied per route because the bulk handlers share a method and
// pattern with cheap ones, so which of them is running is not known until
// after routing has picked between them.
type deadlineStopper struct{ stop func() }

// requestDeadlineMiddleware bounds a request by requestDeadline, at the
// context and at the connection, and gives the handler a way to release it.
//
// The timer is separate from the context so it can be stopped without
// cancelling: a bulk handler needs the cancellation, which is how a client
// going away stops the work, and not the deadline.
func requestDeadlineMiddleware(next http.Handler) http.Handler {
	return deadlineMiddleware(requestDeadline, next)
}

func deadlineMiddleware(within time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		timer := time.AfterFunc(within, cancel)
		defer timer.Stop()

		rc := http.NewResponseController(w)
		setConnDeadlines(r.Context(), rc, time.Now().Add(within))

		stopper := &deadlineStopper{stop: func() {
			timer.Stop()
			setConnDeadlines(r.Context(), rc, time.Time{})
		}}
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, deadlineStopperKey{}, stopper)))
	})
}

// setConnDeadlines applies a read and write deadline to the connection, or
// clears both when t is zero. A protocol that does not support them is not an
// error: the context bound still applies, and h2 streams are already bounded
// by the server's own limits.
func setConnDeadlines(ctx context.Context, rc *http.ResponseController, t time.Time) {
	if err := rc.SetReadDeadline(t); err != nil && !errors.Is(err, http.ErrNotSupported) {
		slog.DebugContext(ctx, "set read deadline", "err", err)
	}
	if err := rc.SetWriteDeadline(t); err != nil && !errors.Is(err, http.ErrNotSupported) {
		slog.DebugContext(ctx, "set write deadline", "err", err)
	}
}

// bulkBody releases the request deadline for the handlers that move object
// data. Those bodies are bounded by progress instead, by the blob client's
// idle guard, because a total cap cannot express "still sending" and a
// multi-gigabyte transfer is legitimately slow.
//
// Cancellation survives, so a client that disconnects still stops the work,
// and the deadline was in force for everything before the handler ran.
func bulkBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stopper, ok := r.Context().Value(deadlineStopperKey{}).(*deadlineStopper); ok {
			stopper.stop()
		}
		next.ServeHTTP(w, r)
	})
}

// setupMiddleware installs the chain that runs before chi has matched a route,
// so nothing here may read the request's bucket or key. Everything that does is
// registered per route group by setupRoutes.
func (s *Server) setupMiddleware() {
	r := s.router

	r.Use(otelsetup.HTTPMiddleware("predastore"))
	r.Use(requestLog)
	r.Use(requestDeadlineMiddleware)
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
}

// throttleMiddleware limits requests per account and action, or returns nil
// when throttling is disabled. It is registered after the resource resolvers,
// which is what lets the action key name the operation being throttled.
func (s *Server) throttleMiddleware() func(http.Handler) http.Handler {
	if s.throttler == nil {
		return nil
	}
	return s.throttler.Middleware(
		[]ratelimit.KeyFunc{
			func(r *http.Request) (string, error) {
				acct := auth.AccountID(r.Context())
				if acct == "" {
					return "", fmt.Errorf("account-id missing from request context")
				}
				return acct, nil
			},
			func(r *http.Request) (string, error) {
				bucket, key := requestBucketKey(r.Context())
				return s3Action(r, bucket, key), nil
			},
		},
		func(w http.ResponseWriter, r *http.Request) {
			handlers.WriteS3Error(w, r, http.StatusServiceUnavailable, "SlowDown",
				"Please reduce your request rate.")
		},
	)
}

// sigV4AuthMiddleware authenticates and authorizes incoming S3 requests:
// public-bucket short-circuit, SigV4 verify, IAM policy eval, and
// cross-account bucket-ownership check.
//
// It authorizes the resource the route resolved, never a re-parse of the URL,
// so the subject it checks and the one the handler acts on cannot diverge.
func (s *Server) sigV4AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		bucket, key := requestBucketKey(r.Context())

		publicBucketAccess := s.cfg.validatePublicBucketPermission(method, bucket)

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
		expectedRegion := s.cfg.Region

		// ListBuckets is the only global operation served here, and clients sign it against
		// us-east-1 whatever region they are configured for. Some SDKs sign it with the
		// configured region instead, so accept either rather than pinning to us-east-1.
		if bucket == "" && sig.Credential.Region == globalSigningRegion {
			expectedRegion = globalSigningRegion
		}

		verified, err := sig.Verify(credResult.SecretAccessKey, expectedRegion, "s3")
		if err != nil {
			handlers.RespondSigV4Error(w, r, accessKey, err, nil)
			return
		}

		action := s3Action(r, bucket, key)
		resource := s3Resource(bucket, key)
		// The batch delete names its keys in the body, which is not read here.
		// Authorizing it against every key in the bucket denies a caller who may
		// delete only some of them, which is the safe direction to be wrong in.
		if isBulkDelete(r, key) {
			resource = s3Resource(bucket, "*")
		}

		// IAM policy evaluation (NATS-sourced credentials only).
		if !credResult.SkipPolicyCheck {
			if action == "" {
				slog.WarnContext(r.Context(), "Unsupported HTTP method for S3 action mapping",
					"method", method, "path", r.URL.Path, "remoteAddr", r.RemoteAddr)
				handlers.WriteS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed")
				return
			}
			if len(credResult.PolicyDocuments) == 0 {
				slog.DebugContext(r.Context(), "No policies resolved for user, implicit deny",
					"accessKeyID", accessKey, "accountID", credResult.AccountID)
			}
			keys := conditionKeys(r, action, credResult)
			if iampolicy.EvaluateWithKeys(action, resource, credResult.PolicyDocuments, keys) == iampolicy.Deny {
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
		isCreateBucket := method == http.MethodPut && bucket != "" && key == "" && r.URL.RawQuery == ""
		if bucket != "" && !isCreateBucket {
			meta, err := handlers.ResolveBucketMetadata(r.Context(), s.cfg.Meta, s.handlerCfg, bucket)
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
					"action", action,
					"resource", resource)
				handlers.WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
		}

		ctx := context.WithValue(r.Context(), auth.ContextKeyAccessKeyID, accessKey)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, credResult.AccountID)
		ctx = context.WithValue(ctx, auth.ContextKeyServiceAccount, credResult.SkipPolicyCheck)
		ctx = handlers.WithSignedPayload(ctx, signedPayload(verified))
		// The transaction span opens before authentication, so the account it
		// resolved to can only be named here. One cluster serves many accounts
		// and S3 is where a tenant's data lives, so an unattributed request is
		// one nobody can be asked about.
		annotateSpanAccount(ctx, credResult.AccountID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// signedPayload is what the write path needs to decode and authenticate a body:
// the sentinel the client signed, and for a signed streaming upload the seed of
// its chunk signature chain.
//
// Only the chain's inputs are carried, never the verified request itself. The
// write path continues one chain; it has no business re-signing anything.
func signedPayload(v *sigv4.VerifiedRequest) handlers.SignedPayload {
	p := handlers.SignedPayload{Signed: true, Mode: v.Canonical.PayloadMode}
	if p.Mode != sigv4.StreamingSigned && p.Mode != sigv4.StreamingSignedTrailer {
		return p
	}
	scope := strings.Join([]string{
		v.Credential.Date, v.Credential.Region, v.Credential.Service, sigv4.AmzScopeTerminator,
	}, "/")
	p.Chain = chunked.NewChain(
		v.SigningKey, v.Signature, scope, v.Timestamp.UTC().Format(sigv4.AmzTimeFormat),
	)
	return p
}
