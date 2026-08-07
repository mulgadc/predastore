package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/predastore/internal/state"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/mulgadc/predastore/otelsetup"
	"github.com/mulgadc/predastore/pkg/iampolicy"
	"github.com/mulgadc/predastore/pkg/sigv4"
	"github.com/mulgadc/predastore/ratelimit"
)

// globalSigningRegion is the region clients sign S3's global operations against,
// independent of the region they are configured for or the endpoint they target.
const globalSigningRegion = "us-east-1"

// Clients are the cluster connections the gateway does its work through. It
// owns neither transport: the process that runs the nodes builds both and
// hands them over.
type Clients struct {
	// State reaches the replicas holding bucket, object and upload metadata.
	State *state.Client
	// Storage reaches the nodes holding shards.
	Storage *storage.Client
}

// HTTP2Server is an HTTP/2 compatible S3 server using net/http. It is the S3
// implementation, not a front end onto one: each handler erasure codes,
// places and records its own operation.
type HTTP2Server struct {
	config    *Config
	router    chi.Router
	server    *http.Server
	credProv  CredentialProvider
	throttler *ratelimit.Throttler

	rsDataShard   int
	rsParityShard int
	hashRing      *consistent.Consistent // shard placement across the storage nodes
	globalState   stateStore             // bucket, object and upload metadata
	shards        *storage.Client        // shard reads and writes
	buckets       []S3_Buckets           // config-defined buckets, plus those created since startup
}

// NewHTTP2Server creates the S3 gateway over the given cluster clients. Shard
// counts and the ring's membership come from the config, so the gateway places
// shards exactly where the cluster it was configured against expects them.
func NewHTTP2Server(config *Config, clients Clients, credProv CredentialProvider) *HTTP2Server {
	dataShards := config.RS.Data
	if dataShards == 0 {
		dataShards = defaultDataShards
	}
	parityShards := config.RS.Parity
	if parityShards == 0 {
		parityShards = defaultParityShards
	}

	s := &HTTP2Server{
		config:        config,
		router:        chi.NewRouter(),
		credProv:      credProv,
		rsDataShard:   dataShards,
		rsParityShard: parityShards,
		hashRing:      newHashRing(config.storageNodeIDs()),
		shards:        clients.Storage,
		buckets:       append([]S3_Buckets(nil), config.Buckets...),
	}
	// Assigned through the nil check so a typed-nil client cannot masquerade as
	// a live one behind the interface.
	if clients.State != nil {
		s.globalState = clients.State
	}

	if config.RateLimit.Enabled {
		s.throttler = ratelimit.New(config.RateLimit)
	}

	s.setupMiddleware()
	s.setupRoutes()
	return s
}

// setupMiddleware installs the chain every request passes through before it
// reaches a handler. chi requires all middleware to be registered before the
// first route, so this runs ahead of setupRoutes.
func (s *HTTP2Server) setupMiddleware() {
	r := s.router

	var logLevel slog.Level
	if s.config.Debug {
		logLevel = slog.LevelDebug
	} else if s.config.DisableLogging {
		logLevel = slog.LevelError
	} else {
		logLevel = slog.LevelInfo
	}

	otelsetup.SetDefaultJSONLogger(logLevel)

	r.Use(otelsetup.HTTPMiddleware("predastore"))
	r.Use(s3SpanMiddleware)
	// chi's access log duplicates the APM transaction from HTTPMiddleware and
	// is a synchronous per-request write on the hot path; only enable it for
	// explicit debug sessions.
	if s.config.Debug {
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
					acct, ok := r.Context().Value(ContextKeyAccountID).(string)
					if !ok || acct == "" {
						return "", fmt.Errorf("account-id missing from request context")
					}
					return acct, nil
				},
				func(r *http.Request) (string, error) {
					return s3Action(r.Method, r.URL.Path), nil
				},
			},
			func(w http.ResponseWriter, r *http.Request) {
				s.writeS3Error(w, r, http.StatusServiceUnavailable, "SlowDown",
					"Please reduce your request rate.")
			},
		))
	}
}

// sigV4AuthMiddleware authenticates and authorizes incoming S3 requests:
// public-bucket short-circuit, SigV4 verify, IAM policy eval, and
// cross-account bucket-ownership check.
func (s *HTTP2Server) sigV4AuthMiddleware(next http.Handler) http.Handler {
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
				s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
			s.respondSigV4Error(w, r, "", err, nil)
			return
		}

		accessKey := sig.Credential.AccessKeyID
		credResult, err := s.credProv.LookupCredentials(accessKey)
		if err != nil {
			s.respondSigV4Error(w, r, accessKey, nil, err)
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
			s.respondSigV4Error(w, r, accessKey, err, nil)
			return
		}

		// IAM policy evaluation (NATS-sourced credentials only).
		if !credResult.SkipPolicyCheck {
			action := s3Action(method, path)
			if action == "" {
				slog.WarnContext(r.Context(), "Unsupported HTTP method for S3 action mapping",
					"method", method, "path", path, "remoteAddr", r.RemoteAddr)
				s.writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed")
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
				s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
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
			meta, err := s.resolveBucketMetadata(bucket)
			if err != nil {
				slog.ErrorContext(r.Context(), "Failed to resolve bucket metadata for ownership check",
					"bucket", bucket, "error", err, "accessKeyID", accessKey)
				s.writeS3Error(w, r, http.StatusInternalServerError, "InternalError",
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
				s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
				return
			}
		}

		ctx := context.WithValue(r.Context(), ContextKeyAccessKeyID, accessKey)
		ctx = context.WithValue(ctx, ContextKeyAccountID, credResult.AccountID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ListenAndServe starts the HTTP/2 server with TLS.
func (s *HTTP2Server) ListenAndServe(addr, certFile, keyFile string) error {
	// Load TLS certificates
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	// Configure TLS with HTTP/2 support
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		// NextProtos enables ALPN for HTTP/2 negotiation
		// "h2" = HTTP/2, "http/1.1" = HTTP/1.1 fallback
		NextProtos:       []string{"h2", "http/1.1"},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}

	s.server = &http.Server{
		Addr:      addr,
		Handler:   s.router,
		TLSConfig: tlsConfig,
		// Timeouts
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		// Max header size
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	slog.Info("Starting HTTP/2 S3 server", "addr", addr, "http2", true)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	tlsListener := tls.NewListener(ln, tlsConfig)
	return s.server.Serve(tlsListener)
}

// ListenAndServeAsync starts the server in a goroutine.
func (s *HTTP2Server) ListenAndServeAsync(addr, certFile, keyFile string) error {
	go func() {
		if err := s.ListenAndServe(addr, certFile, keyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP/2 server error", "error", err)
		}
	}()
	return nil
}

// Shutdown gracefully shuts down the server.
func (s *HTTP2Server) Shutdown(ctx context.Context) error {
	if s.throttler != nil {
		s.throttler.Stop()
	}
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// GetRouter returns the chi router for testing.
func (s *HTTP2Server) GetRouter() chi.Router {
	return s.router
}

// GetHandler returns the HTTP handler for testing with httptest.
func (s *HTTP2Server) GetHandler() http.Handler {
	return s.router
}
