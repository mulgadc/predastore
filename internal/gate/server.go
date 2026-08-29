package gate

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/bluebottle/pkg/ratelimit"
	"github.com/mulgadc/bluebottle/pkg/tlsconfig"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/gate/repair"
)

const (
	defaultAddr = "0.0.0.0"
	defaultPort = 8443

	// shutdownGrace bounds how long Run waits for in-flight requests to finish
	// after the context is cancelled.
	shutdownGrace = 10 * time.Second
)

// Server is predastore's S3 gate: the HTTPS listener, the SigV4 + IAM
// middleware chain and the route table. It is the S3 implementation, not a
// front end onto one — the handlers it routes to erasure code, place and
// record their own operations.
type Server struct {
	cfg Config
	// cert is loaded by New, so a gate that cannot serve TLS fails to build
	// rather than at its first request.
	cert tls.Certificate

	router    chi.Router
	credProv  auth.CredentialProvider
	throttler *ratelimit.Throttler

	// Handler dependencies, shared by the route table and the auth middleware.
	handlerCfg handlers.Config
	buckets    *handlers.BucketCache // config-defined buckets, plus those created since startup

	// repairer is nil unless repair is enabled and this gate has local blob
	// nodes to repair for.
	repairer *repair.Service
}

var _ http.Handler = (*Server)(nil)

// New validates cfg, applies its defaults, resolves the credential chain and
// assembles the route table. It binds nothing and starts no goroutine: the
// listener is Run's, and nothing on the server is set after this returns.
func New(cfg Config) (*Server, error) {
	if cfg.Region == "" {
		return nil, errors.New("region is required")
	}
	// The clients arrive fully wired from the process that owns the cluster
	// nodes; the gate never launches storage or state itself.
	if cfg.Meta == nil {
		return nil, errors.New("no metadata client: build the cluster runtime and set Config.Meta")
	}
	if cfg.Blob == nil {
		return nil, errors.New("no blob client: build the cluster runtime and set Config.Blob")
	}
	// TLS is a construction input, not a serving one: a gate that cannot load
	// its certificate says so now rather than at the first request.
	if cfg.TLSCert == "" || cfg.TLSKey == "" {
		return nil, errors.New("TLS is required: set Config.TLSCert and Config.TLSKey")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	if cfg.Addr == "" {
		cfg.Addr = defaultAddr
	}
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}

	credProv := cfg.CredProv
	if credProv == nil {
		var perr error
		if credProv, perr = newCredentialProvider(cfg); perr != nil {
			return nil, fmt.Errorf("failed to initialize credential provider: %w", perr)
		}
	}

	handlerCfg := cfg.handlerConfig()

	// One minter per gate process. Building it here rather than in
	// handlerConfig matters: that method is also called to look a bucket up,
	// and a second minter for the same node would reissue the same epochs.
	minter, err := handlers.NewEpochMinter(cfg.NodeID)
	if err != nil {
		return nil, err
	}
	handlerCfg.Epochs = minter
	s := &Server{
		cfg:        cfg,
		cert:       cert,
		router:     chi.NewRouter(),
		credProv:   credProv,
		handlerCfg: handlerCfg,
		buckets:    handlers.NewBucketCache(handlerCfg.Buckets),
	}
	if cfg.RateLimit.Enabled {
		s.throttler = ratelimit.New(cfg.RateLimit)
	}

	ring := placement.NewRing(cfg.BlobNodeIDs)
	if s.repairer, err = newRepairer(cfg, ring); err != nil {
		return nil, err
	}

	s.setupMiddleware()
	s.setupRoutes(ring)
	return s, nil
}

// newRepairer builds the background sweep, or returns nil when there is nothing
// for it to do. A gate with no colocated blob nodes is not a failure to
// configure: it repairs for the nodes sharing its process, and a gate-only host
// legitimately has none.
func newRepairer(cfg Config, ring *placement.Ring) (*repair.Service, error) {
	if !cfg.Repair.Enabled || len(cfg.LocalBlobNodeIDs) == 0 {
		return nil, nil
	}

	svc, err := repair.New(repair.Config{
		Nodes:        cfg.LocalBlobNodeIDs,
		Ring:         ring,
		Meta:         cfg.Meta,
		Blob:         cfg.Blob,
		DataShards:   cfg.RS.Data,
		ParityShards: cfg.RS.Parity,
		Workers:      cfg.Repair.Workers,
		PageSize:     cfg.Repair.PageSize,
		Interval:     cfg.Repair.Interval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build the repair sweep: %w", err)
	}

	return svc, nil
}

// ServeHTTP routes one S3 request through the middleware chain. Run serves
// this same handler over TLS; a caller holding a Server can drive it directly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// newCredentialProvider resolves the auth chain: NATS-backed IAM when it is
// configured, with the config-defined accounts always as the fallback.
func newCredentialProvider(config Config) (auth.CredentialProvider, error) {
	configProv := auth.NewConfigProvider(config.Auth)

	if config.IAM == nil {
		slog.Info("IAM not configured, using config-only auth")
		return configProv, nil
	}

	natsProv, err := auth.NewNATSIAMProvider(config.IAM)
	if err != nil {
		return nil, fmt.Errorf("IAM configured but NATS provider failed to initialize: %w", err)
	}

	slog.Info("Using NATS IAM + config chain auth")
	return auth.NewChainProvider(natsProv, configProv), nil
}

// alpnProtocols is what the gate offers over ALPN. h2 is offered only when
// asked for: a client multiplexes every request onto one h2 connection, so a
// single large PUT exhausts the connection's flow-control window and stalls
// the small ranged GETs sharing it. Offering only http/1.1 leaves such a
// client on a connection pool, where each request gets its own socket.
func alpnProtocols(enableHTTP2 bool) []string {
	if enableHTTP2 {
		return []string{"h2", "http/1.1"}
	}
	return []string{"http/1.1"}
}

// httpProtocols mirrors alpnProtocols for the server's own wiring. Both are
// needed: Serve installs the h2 handler whenever TLSConfig mentions h2, so
// leaving this unset would contradict the ALPN list above.
// newHTTPServer builds the S3 listener's server.
//
// It sets no ReadTimeout or WriteTimeout deliberately. Both are whole-body
// bounds by construction, so neither can express "the peer is still sending"
// and either would cap an object at whatever transfers inside it — a 60s
// WriteTimeout is a few gigabytes and no more. requestDeadlineMiddleware
// applies the equivalent per request and releases it for object data, whose
// bodies are bounded by progress instead.
//
// The bounds that remain are the ones on fixed-size work: the header
// exchange, and an idle connection nobody is using.
func newHTTPServer(addr string, handler http.Handler, tlsCfg *tls.Config, protocols *http.Protocols) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         tlsCfg,
		Protocols:         protocols,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func httpProtocols(enableHTTP2 bool) *http.Protocols {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetHTTP2(enableHTTP2)
	return &p
}

// Run serves S3 over TLS until ctx is cancelled or the listener fails, then
// drains in-flight requests within the grace period and releases everything it
// started. It is the server's whole lifecycle: there is nothing to stop it
// with but the context.
func (s *Server) Run(ctx context.Context) error {
	// Teardown runs LIFO, so registration order here is the reverse of the
	// order things are released in: the listener drains first, then the
	// credential provider and the throttler that fronted it.
	if s.throttler != nil {
		defer s.throttler.Stop()
	}
	if s.credProv != nil {
		defer s.credProv.Close()
	}

	tlsCfg := &tls.Config{
		Certificates:     []tls.Certificate{s.cert},
		NextProtos:       alpnProtocols(s.cfg.EnableHTTP2),
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}
	protocols := httpProtocols(s.cfg.EnableHTTP2)

	addr := net.JoinHostPort(s.cfg.Addr, strconv.Itoa(s.cfg.Port))
	httpSrv := newHTTPServer(addr, s.router, tlsCfg, protocols)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	// Buffered so the send never blocks, and closed after it so the drain below
	// can wait for Serve to return whether or not the select already took the
	// value. No goroutine outlives Run.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpSrv.Serve(tls.NewListener(ln, tlsCfg))
		close(serveErr)
	}()

	defer func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := httpSrv.Shutdown(drainCtx); err != nil {
			slog.Error("S3 gate did not drain within grace period", "error", err)
		}
		<-serveErr
	}()

	// The sweep is scoped to Run so no goroutine outlives it, and it is stopped
	// before the listener drains: a rebuild in flight holds streams to peers
	// that the drain would otherwise wait behind.
	if s.repairer != nil {
		repairCtx, stopRepair := context.WithCancel(ctx)
		var repairDone sync.WaitGroup
		repairDone.Go(func() {
			if err := s.repairer.Run(repairCtx); err != nil {
				slog.Error("Repair sweep stopped", "error", err)
			}
		})
		defer func() {
			stopRepair()
			repairDone.Wait()
		}()
	}

	slog.Info("Starting S3 gate", "addr", addr, "http2", s.cfg.EnableHTTP2)

	select {
	case <-ctx.Done():
		slog.Info("Shutting down S3 gate...")
		return nil
	case err := <-serveErr:
		// A gate that cannot bind must take the process down rather than
		// leave the cluster running headless.
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
