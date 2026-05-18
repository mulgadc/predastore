// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/mulgadc/predastore/auth"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/mulgadc/predastore/ratelimit"
	"github.com/mulgadc/predastore/s3/chunked"
)

// HTTP2Server is an HTTP/2 compatible S3 server using net/http
type HTTP2Server struct {
	config    *Config
	backend   backend.Backend
	router    chi.Router
	server    *http.Server
	credProv  CredentialProvider
	throttler *ratelimit.Throttler
}

// NewHTTP2Server creates a new HTTP/2 compatible S3 server
func NewHTTP2Server(config *Config) *HTTP2Server {
	s := &HTTP2Server{
		config:   config,
		router:   chi.NewRouter(),
		credProv: NewConfigProvider(config.Auth),
	}

	if config.RateLimit.Enabled {
		s.throttler = ratelimit.New(config.RateLimit)
	}

	// Create backend based on config
	if len(config.Nodes) > 0 {
		s.backend = s.createDistributedBackend()
	} else {
		slog.Warn("No [[nodes]] configured, starting without storage backend")
	}

	s.setupRoutes()
	return s
}

// NewHTTP2ServerWithBackend creates a new HTTP/2 server with an existing backend
func NewHTTP2ServerWithBackend(config *Config, be backend.Backend, credProv CredentialProvider) *HTTP2Server {
	s := &HTTP2Server{
		config:   config,
		backend:  be,
		router:   chi.NewRouter(),
		credProv: credProv,
	}

	if config.RateLimit.Enabled {
		s.throttler = ratelimit.New(config.RateLimit)
	}

	s.setupRoutes()
	return s
}

func (s *HTTP2Server) createDistributedBackend() backend.Backend {
	nodes := make([]distributed.NodeConfig, 0, len(s.config.Nodes))
	for _, n := range s.config.Nodes {
		nodes = append(nodes, distributed.NodeConfig{
			ID:     n.ID,
			Host:   n.Host,
			Port:   n.Port,
			Path:   n.Path,
			DB:     n.DB,
			DBPort: n.DBPort,
			DBPath: n.DBPath,
			Leader: n.Leader,
			Epoch:  n.Epoch,
		})
	}

	buckets := make([]distributed.BucketConfig, 0, len(s.config.Buckets))
	for _, b := range s.config.Buckets {
		buckets = append(buckets, distributed.BucketConfig{
			Name:      b.Name,
			Region:    b.Region,
			Type:      b.Type,
			Public:    b.Public,
			AccountID: b.AccountID,
		})
	}

	config := &distributed.Config{
		BadgerDir:    s.config.BadgerDir,
		DataShards:   s.config.RS.Data,
		ParityShards: s.config.RS.Parity,
		Nodes:        nodes,
		Buckets:      buckets,
	}

	be, err := distributed.New(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create distributed backend: %v", err))
	}
	return be
}

func (s *HTTP2Server) setupRoutes() {
	r := s.router

	// Configure logging
	var logLevel slog.Level
	if s.config.Debug {
		logLevel = slog.LevelDebug
	} else if s.config.DisableLogging {
		logLevel = slog.LevelError
	} else {
		logLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(handler))

	// Middleware
	if !s.config.DisableLogging {
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

	// Routes
	r.Get("/", s.listBuckets)

	// Bucket operations (without key)
	r.Put("/{bucket}", s.createBucket)
	r.Head("/{bucket}", s.headBucket)
	r.Delete("/{bucket}", s.deleteBucket)
	r.Get("/{bucket}", s.listObjects)

	// Object operations (with key)
	r.Head("/{bucket}/*", s.headObject)
	r.Get("/{bucket}/*", s.getObject)
	r.Put("/{bucket}/*", s.putObject)
	r.Post("/{bucket}/*", s.postObject)
	r.Delete("/{bucket}/*", s.deleteObject)
}

// sigV4AuthMiddleware authenticates and authorizes incoming S3 requests.
// Stages: public-bucket short-circuit → SigV4 verification → IAM policy
// evaluation → cross-account bucket-ownership check → handler dispatch
// with the authenticated principal stored on the request context.
func (s *HTTP2Server) sigV4AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method

		// Public buckets short-circuit auth when no Authorization header
		// is presented. A presented header always goes through the verifier
		// so signature failures still produce an explicit error.
		publicBucketAccess := s.config.validatePublicBucketPermission(method, path)
		if r.Header.Get("Authorization") == "" {
			if publicBucketAccess == nil {
				next.ServeHTTP(w, r)
				return
			}
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}

		// Closure-captured: the SecretLookup callback runs inside
		// VerifySigV4Request and is the only point credResult is in scope
		// before the request continues. We stash it here so the IAM and
		// ownership stages downstream can consume it without re-fetching.
		var (
			credResult *CredentialResult
			lookupErr  error
		)
		lookup := func(id string) (string, error) {
			cr, err := s.credProv.LookupCredentials(id)
			if err != nil {
				lookupErr = err
				return "", err
			}
			credResult = cr
			return cr.SecretAccessKey, nil
		}

		accessKey, err := auth.VerifySigV4Request(r, s.config.Region, "s3", lookup)
		if err != nil {
			s.respondSigV4Error(w, r, accessKey, err, lookupErr)
			return
		}

		// IAM policy evaluation (NATS-sourced credentials only).
		if !credResult.SkipPolicyCheck {
			action := s3Action(method, path)
			if action == "" {
				slog.Warn("Unsupported HTTP method for S3 action mapping",
					"method", method, "path", path, "remoteAddr", r.RemoteAddr)
				s.writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed")
				return
			}
			resource := s3Resource(path)
			if len(credResult.PolicyDocuments) == 0 {
				slog.Debug("No policies resolved for user, implicit deny",
					"accessKeyID", accessKey, "accountID", credResult.AccountID)
			}
			if !evaluateS3Access(action, resource, credResult.PolicyDocuments) {
				slog.Debug("S3 access denied by policy",
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
				slog.Error("Failed to resolve bucket metadata for ownership check",
					"bucket", bucket, "error", err, "accessKeyID", accessKey)
				s.writeS3Error(w, r, http.StatusInternalServerError, "InternalError",
					"An internal error occurred")
				return
			}
			// Unknown bucket — let the route handler return NoSuchBucket so
			// existence is reported consistently with non-authenticated paths.
			if meta != nil && !bucketAccessAllowed(method, credResult.AccountID, meta, credResult.SkipPolicyCheck) {
				slog.Warn("Cross-account bucket access denied",
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

// respondSigV4Error maps an auth-package sentinel (or a SecretLookup
// failure that propagated out via lookupErr) to the appropriate S3 error
// response. *SigMismatchError is unwrapped via errors.As to surface
// canonical-request + string-to-sign in the warn log for SDK debugging.
func (s *HTTP2Server) respondSigV4Error(w http.ResponseWriter, r *http.Request, claimedKey string, err, lookupErr error) {
	// SecretLookup errors are returned verbatim from VerifySigV4Request;
	// route them via the original lookup error so callers see ErrKeyNotFound
	// distinct from the auth-package sentinels below.
	if lookupErr != nil {
		if errors.Is(lookupErr, ErrKeyNotFound) {
			slog.Warn("Unknown access key", "accessKeyID", claimedKey, "remoteAddr", r.RemoteAddr)
			s.writeS3Error(w, r, http.StatusForbidden, "InvalidAccessKeyId",
				"The AWS Access Key Id you provided does not exist in our records")
			return
		}
		slog.Error("Credential lookup infrastructure error",
			"accessKeyID", claimedKey, "error", lookupErr, "remoteAddr", r.RemoteAddr)
		s.writeS3Error(w, r, http.StatusInternalServerError, "InternalError",
			"An internal error occurred while validating credentials")
		return
	}

	switch {
	case errors.Is(err, auth.ErrMissingAuth):
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Missing Authorization header")
	case errors.Is(err, auth.ErrInvalidAuthFormat):
		// RequireSignedHeaders failures wrap ErrInvalidAuthFormat with a
		// message that names the offending header. Map those to
		// AuthorizationHeaderMalformed so SDKs see the same code AWS uses.
		if strings.Contains(err.Error(), "SignedHeaders") {
			s.writeS3Error(w, r, http.StatusForbidden, "AuthorizationHeaderMalformed", err.Error())
			return
		}
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid Authorization header format")
	case errors.Is(err, auth.ErrInvalidCredScope):
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid credential scope")
	case errors.Is(err, auth.ErrMissingDate):
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Missing required header: X-Amz-Date")
	case errors.Is(err, auth.ErrInvalidDateFormat):
		slog.Debug("Invalid X-Amz-Date format", "timestamp", r.Header.Get("X-Amz-Date"))
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid X-Amz-Date header format")
	case errors.Is(err, auth.ErrClockSkew):
		slog.Debug("Request timestamp outside allowed skew", "timestamp", r.Header.Get("X-Amz-Date"))
		s.writeS3Error(w, r, http.StatusForbidden, "RequestTimeTooSkewed",
			"The difference between the request time and the current time is too large")
	case errors.Is(err, auth.ErrBodyTooLarge):
		slog.Warn("Request body exceeds auth size limit", "accessKeyID", claimedKey)
		s.writeS3Error(w, r, http.StatusRequestEntityTooLarge, "EntityTooLarge",
			"Request body exceeds signature validation size limit")
	case errors.Is(err, auth.ErrSignatureMismatch):
		var smErr *auth.SigMismatchError
		if errors.As(err, &smErr) {
			slog.Warn("SigV4 signature mismatch",
				"accessKeyID", smErr.AccessKeyID,
				"method", r.Method,
				"path", r.URL.Path,
				"host", r.Host,
				"amzDate", r.Header.Get("X-Amz-Date"),
				"payloadHashHeader", r.Header.Get("X-Amz-Content-Sha256"),
				"contentLength", r.Header.Get("Content-Length"),
				"userAgent", r.Header.Get("User-Agent"),
				"proto", r.Proto,
				"remoteAddr", r.RemoteAddr,
				"expectedSigPrefix", smErr.ExpectedSigPrefix,
				"providedSigPrefix", smErr.ProvidedSigPrefix,
				"canonicalRequest", smErr.CanonicalRequest,
				"stringToSign", smErr.StringToSign,
			)
		}
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "The request signature does not match")
	default:
		slog.Warn("Unexpected SigV4 verification error", "error", err, "accessKeyID", claimedKey)
		s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", err.Error())
	}
}

// writeS3Error writes an S3 error response
func (s *HTTP2Server) writeS3Error(w http.ResponseWriter, r *http.Request, statusCode int, code, message string) {
	s3error := S3Error{
		Code:      code,
		Message:   message,
		RequestId: uuid.NewString(),
		HostId:    r.Host,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	if err := xml.NewEncoder(w).Encode(s3error); err != nil {
		slog.Debug("failed to encode XML error response", "error", err)
	}
}

// writeXML writes an XML response
func (s *HTTP2Server) writeXML(w http.ResponseWriter, statusCode int, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	return xml.NewEncoder(w).Encode(v)
}

// handleError converts backend errors to S3 error responses
func (s *HTTP2Server) handleError(w http.ResponseWriter, r *http.Request, err error) {
	statusCode := http.StatusInternalServerError
	var s3error S3Error

	if backendErr, ok := backend.IsS3Error(err); ok {
		statusCode = backendErr.StatusCode
		s3error.Code = string(backendErr.Code)
		s3error.Message = backendErr.Message
	} else {
		switch {
		case strings.Contains(err.Error(), "NoSuchBucket") || strings.Contains(err.Error(), "Bucket not found"):
			statusCode = http.StatusNotFound
			s3error.Code = "NoSuchBucket"
			s3error.Message = "The specified bucket does not exist"
		case strings.Contains(err.Error(), "AccessDenied"):
			statusCode = http.StatusForbidden
			s3error.Code = "AccessDenied"
			s3error.Message = "Access Denied"
		case strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "not found") || errors.Is(err, os.ErrNotExist):
			statusCode = http.StatusNotFound
			s3error.Code = "NoSuchKey"
			s3error.Message = "The specified key does not exist"
		default:
			s3error.Code = "InternalError"
			s3error.Message = err.Error()
		}
	}

	s3error.RequestId = uuid.NewString()
	s3error.HostId = r.Host

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	if err := xml.NewEncoder(w).Encode(s3error); err != nil {
		slog.Debug("failed to encode XML error response", "error", err)
	}
}

// Route handlers

func (s *HTTP2Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountID := ""
	if v := ctx.Value(ContextKeyAccountID); v != nil {
		accountID, _ = v.(string)
	}

	resp, err := s.backend.ListBuckets(ctx, accountID)
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	result := ListBuckets{
		Owner: BucketOwner{
			ID:          resp.Owner.ID,
			DisplayName: resp.Owner.DisplayName,
		},
	}
	for _, b := range resp.Buckets {
		result.Buckets = append(result.Buckets, ListBucket{
			Name:         b.Name,
			CreationDate: b.CreationDate,
		})
	}

	if err := s.writeXML(w, http.StatusOK, result); err != nil {
		slog.Debug("failed to write XML response", "error", err)
	}
}

func (s *HTTP2Server) createBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")

	// PUT /{bucket}?policy — bucket policies are not supported
	if r.URL.Query().Has("policy") {
		s.writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "Bucket policy is not implemented")
		return
	}

	ownerID := ""
	if v := ctx.Value(ContextKeyAccessKeyID); v != nil {
		ownerID, _ = v.(string)
	}
	accountID := ""
	if v := ctx.Value(ContextKeyAccountID); v != nil {
		accountID, _ = v.(string)
	}

	region := s.config.Region
	if r.ContentLength > 0 {
		var config CreateBucketConfiguration
		body, _ := io.ReadAll(r.Body)
		if xml.Unmarshal(body, &config) == nil && config.LocationConstraint != "" {
			region = config.LocationConstraint
		}
	}

	_, err := s.backend.CreateBucket(ctx, &backend.CreateBucketRequest{
		Bucket:           bucket,
		Region:           region,
		OwnerID:          ownerID,
		AccountID:        accountID,
		OwnerDisplayName: ownerID,
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("http://%s.s3.%s.amazonaws.com/", bucket, region))
	w.WriteHeader(http.StatusOK)
}

func (s *HTTP2Server) headBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")

	resp, err := s.backend.HeadBucket(ctx, &backend.HeadBucketRequest{Bucket: bucket})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.Header().Set("X-Amz-Bucket-Region", resp.Region)
	w.WriteHeader(http.StatusOK)
}

func (s *HTTP2Server) deleteBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")

	// DELETE /{bucket}?policy — no-op, bucket policies are not supported
	if r.URL.Query().Has("policy") {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	ownerID := ""
	if v := ctx.Value(ContextKeyAccessKeyID); v != nil {
		ownerID, _ = v.(string)
	}

	err := s.backend.DeleteBucket(ctx, &backend.DeleteBucketRequest{
		Bucket:  bucket,
		OwnerID: ownerID,
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *HTTP2Server) listObjects(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	query := r.URL.Query()

	// Return proper errors for unsupported bucket sub-resource operations
	// that Terraform and other tools may call.
	slog.Debug("listObjects called", "bucket", bucket, "query", r.URL.RawQuery)
	if query.Has("policy") {
		slog.Debug("returning NoSuchBucketPolicy for ?policy request", "bucket", bucket)
		s.writeS3Error(w, r, http.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
		return
	}
	if query.Has("acl") {
		s.writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "ACL is not implemented")
		return
	}
	if query.Has("versioning") {
		s.writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "Versioning is not implemented")
		return
	}

	resp, err := s.backend.ListObjects(ctx, &backend.ListObjectsRequest{
		Bucket:    bucket,
		Prefix:    query.Get("prefix"),
		Delimiter: query.Get("delimiter"),
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	contents := make([]ListObjectsV2_Contents, 0, len(resp.Contents))
	for _, obj := range resp.Contents {
		contents = append(contents, ListObjectsV2_Contents{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}

	prefixes := make([]ListObjectsV2_Dir, 0, len(resp.CommonPrefixes))
	for _, p := range resp.CommonPrefixes {
		prefixes = append(prefixes, ListObjectsV2_Dir{Prefix: p})
	}

	result := ListObjectsV2{
		Name:           resp.Name,
		Prefix:         resp.Prefix,
		KeyCount:       resp.KeyCount,
		MaxKeys:        resp.MaxKeys,
		IsTruncated:    resp.IsTruncated,
		Contents:       &contents,
		CommonPrefixes: &prefixes,
	}

	if err := s.writeXML(w, http.StatusOK, result); err != nil {
		slog.Debug("failed to write XML response", "error", err)
	}
}

func (s *HTTP2Server) headObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	resp, err := s.backend.HeadObject(ctx, bucket, key)
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", resp.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	w.Header().Set("ETag", resp.ETag)
	w.Header().Set("Last-Modified", resp.LastModified.Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	w.WriteHeader(http.StatusOK)
}

func (s *HTTP2Server) getObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	req := &backend.GetObjectRequest{
		Bucket:     bucket,
		Key:        key,
		RangeStart: -1,
		RangeEnd:   -1,
	}

	// Parse Range header
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		if strings.HasPrefix(rangeHeader, "bytes=") {
			rangeSpec := rangeHeader[6:]
			if idx := strings.Index(rangeSpec, "-"); idx >= 0 {
				if idx > 0 {
					start, _ := strconv.ParseInt(rangeSpec[:idx], 10, 64)
					req.RangeStart = start
				}
				if idx < len(rangeSpec)-1 {
					end, _ := strconv.ParseInt(rangeSpec[idx+1:], 10, 64)
					req.RangeEnd = end
				}
			}
		}
	}

	resp, err := s.backend.GetObject(ctx, req)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(resp.Size, 10))
	w.Header().Set("ETag", resp.ETag)
	w.Header().Set("Last-Modified", resp.LastModified.Format("Mon, 02 Jan 2006 15:04:05 GMT"))

	if resp.StatusCode == http.StatusPartialContent {
		w.Header().Set("Content-Range", resp.ContentRange)
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		slog.Debug("failed to copy response body", "error", err)
	}
}

func (s *HTTP2Server) putObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	// Check for multipart part upload
	if partNum := r.URL.Query().Get("partNumber"); partNum != "" {
		uploadID := r.URL.Query().Get("uploadId")
		partNumber, _ := strconv.Atoi(partNum)
		decodedLen, _ := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)

		resp, err := s.backend.UploadPart(ctx, &backend.UploadPartRequest{
			Bucket:          bucket,
			Key:             key,
			UploadID:        uploadID,
			PartNumber:      partNumber,
			Body:            chunked.NewHTTPBodyReader(r),
			ContentEncoding: r.Header.Get("Content-Encoding"),
			IsChunked:       r.Header.Get("Content-Encoding") == "aws-chunked",
			DecodedLength:   decodedLen,
		})
		if err != nil {
			s.handleError(w, r, err)
			return
		}

		w.Header().Set("ETag", resp.ETag)
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		w.WriteHeader(http.StatusOK)
		return
	}

	// Regular put object
	decodedLen, _ := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)

	resp, err := s.backend.PutObject(ctx, &backend.PutObjectRequest{
		Bucket:          bucket,
		Key:             key,
		Body:            chunked.NewHTTPBodyReader(r),
		ContentEncoding: r.Header.Get("Content-Encoding"),
		IsChunked:       r.Header.Get("Content-Encoding") == "aws-chunked",
		DecodedLength:   decodedLen,
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.Header().Set("ETag", resp.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *HTTP2Server) postObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	uploadID := r.URL.Query().Get("uploadId")
	if uploadID == "" {
		// Create multipart upload
		resp, err := s.backend.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			s.handleError(w, r, err)
			return
		}

		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		if err := s.writeXML(w, http.StatusOK, InitiateMultipartUploadResult{
			Bucket:   resp.Bucket,
			Key:      resp.Key,
			UploadId: resp.UploadID,
		}); err != nil {
			slog.Debug("failed to write XML response", "error", err)
		}
		return
	}

	// Complete multipart upload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	var completeReq CompleteMultipartUpload
	if err := xml.Unmarshal(body, &completeReq); err != nil {
		s.handleError(w, r, err)
		return
	}

	parts := make([]backend.CompletedPart, len(completeReq.Parts))
	for i, p := range completeReq.Parts {
		parts[i] = backend.CompletedPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	resp, err := s.backend.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts:    parts,
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	if err := s.writeXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Location: fmt.Sprintf("https://%s%s", r.Host, resp.Location),
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		ETag:     resp.ETag,
	}); err != nil {
		slog.Debug("failed to write XML response", "error", err)
	}
}

func (s *HTTP2Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")

	err := s.backend.DeleteObject(ctx, &backend.DeleteObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		s.handleError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListenAndServe starts the HTTP/2 server with TLS
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

// ListenAndServeAsync starts the server in a goroutine
func (s *HTTP2Server) ListenAndServeAsync(addr, certFile, keyFile string) error {
	go func() {
		if err := s.ListenAndServe(addr, certFile, keyFile); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP/2 server error", "error", err)
		}
	}()
	return nil
}

// Shutdown gracefully shuts down the server
func (s *HTTP2Server) Shutdown(ctx context.Context) error {
	if s.throttler != nil {
		s.throttler.Stop()
	}
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// GetRouter returns the chi router for testing
func (s *HTTP2Server) GetRouter() chi.Router {
	return s.router
}

// GetHandler returns the HTTP handler for testing with httptest
func (s *HTTP2Server) GetHandler() http.Handler {
	return s.router
}

// resolveBucketMetadata returns metadata for the named bucket. Config-defined
// buckets (static, known at startup) are checked first to avoid a synchronous
// backend round-trip on every authenticated request. Returns nil with no error
// when the bucket is unknown anywhere — the route handler is responsible for
// returning NoSuchBucket so existence is reported consistently.
func (s *HTTP2Server) resolveBucketMetadata(bucket string) (*backend.BucketMetadata, error) {
	if b, err := s.config.BucketConfig(bucket); err == nil {
		return &backend.BucketMetadata{
			Name:      b.Name,
			Region:    b.Region,
			AccountID: b.AccountID,
			Public:    b.Public,
		}, nil
	}
	if s.backend == nil {
		return nil, nil
	}
	meta, err := s.backend.GetBucketMetadata(bucket)
	if err == nil {
		return meta, nil
	}
	if backendErr, ok := backend.IsS3Error(err); ok && backendErr.Code == backend.ErrNoSuchBucket {
		return nil, nil
	}
	return nil, err
}
