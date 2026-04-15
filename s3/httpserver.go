// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/mulgadc/predastore/auth"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/mulgadc/predastore/s3/chunked"
)

// maxClockSkew is the maximum allowed difference between the request
// timestamp and the server's current time. Matches spinifex gateway (5 min).
const maxClockSkew = 5 * time.Minute

// HTTP2Server is an HTTP/2 compatible S3 server using net/http
type HTTP2Server struct {
	config   *Config
	backend  backend.Backend
	router   chi.Router
	server   *http.Server
	credProv CredentialProvider
}

// NewHTTP2Server creates a new HTTP/2 compatible S3 server
func NewHTTP2Server(config *Config) *HTTP2Server {
	s := &HTTP2Server{
		config:   config,
		router:   chi.NewRouter(),
		credProv: NewConfigProvider(config.Auth),
	}

	// Create backend based on config
	if len(config.Nodes) > 0 {
		s.backend = s.createDistributedBackend()
	} else {
		s.backend = s.createFilesystemBackend()
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
	s.setupRoutes()
	return s
}

func (s *HTTP2Server) createFilesystemBackend() backend.Backend {
	buckets := make([]filesystem.BucketConfig, 0, len(s.config.Buckets))
	for _, b := range s.config.Buckets {
		buckets = append(buckets, filesystem.BucketConfig{
			Name:     b.Name,
			Pathname: b.Pathname,
			Region:   b.Region,
			Type:     b.Type,
			Public:   b.Public,
		})
	}

	config := &filesystem.Config{Buckets: buckets}
	be, err := filesystem.New(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create filesystem backend: %v", err))
	}
	return be
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
			Name:   b.Name,
			Region: b.Region,
			Type:   b.Type,
			Public: b.Public,
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

// sigV4AuthMiddleware validates AWS Signature V4 authentication
func (s *HTTP2Server) sigV4AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method
		authHeader := r.Header.Get("Authorization")

		// Check if resource is public
		publicBucketAccess := s.config.validatePublicBucketPermission(method, path)

		// Allow public bucket access if no auth header
		if publicBucketAccess == nil && authHeader == "" {
			next.ServeHTTP(w, r)
			return
		}

		// If not public and no auth header, deny access
		if authHeader == "" {
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Access Denied")
			return
		}

		// Parse authorization header
		parts := strings.Split(authHeader, ", ")
		if len(parts) != 3 {
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid Authorization header format")
			return
		}

		creds := strings.Split(strings.TrimPrefix(parts[0], "AWS4-HMAC-SHA256 Credential="), "/")
		if len(creds) != 5 {
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid credential scope")
			return
		}

		accessKey, date, region, svc := creds[0], creds[1], creds[2], creds[3]

		// Validate X-Amz-Date timestamp to prevent replay attacks
		amzDate := r.Header.Get("X-Amz-Date")
		if amzDate == "" {
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Missing required header: X-Amz-Date")
			return
		}
		parsedTime, err := time.Parse(auth.TimeFormat, amzDate)
		if err != nil {
			slog.Debug("Invalid X-Amz-Date format", "timestamp", amzDate)
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "Invalid X-Amz-Date header format")
			return
		}
		if time.Since(parsedTime).Abs() > maxClockSkew {
			slog.Debug("Request timestamp outside allowed skew",
				"timestamp", amzDate, "skew", time.Since(parsedTime))
			s.writeS3Error(w, r, http.StatusForbidden, "RequestTimeTooSkewed",
				"The difference between the request time and the current time is too large")
			return
		}

		credResult, err := s.credProv.LookupCredentials(accessKey)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				slog.Warn("Unknown access key",
					"accessKeyID", accessKey,
					"remoteAddr", r.RemoteAddr)
				s.writeS3Error(w, r, http.StatusForbidden, "InvalidAccessKeyId",
					"The AWS Access Key Id you provided does not exist in our records")
			} else {
				// Infrastructure error (NATS down, etc.) — return 500 so AWS SDKs retry.
				slog.Error("Credential lookup infrastructure error",
					"accessKeyID", accessKey,
					"error", err,
					"remoteAddr", r.RemoteAddr)
				s.writeS3Error(w, r, http.StatusInternalServerError, "InternalError",
					"An internal error occurred while validating credentials")
			}
			return
		}
		secretKey := credResult.SecretAccessKey

		signedHeaders := strings.TrimPrefix(parts[1], "SignedHeaders=")
		signature := strings.TrimPrefix(parts[2], "Signature=")

		// Build canonical request
		canonicalURI := r.URL.Path
		if canonicalURI == "" {
			canonicalURI = "/"
		}
		canonicalURI = auth.UriEncode(canonicalURI, false)

		// Canonical query string
		queryUrl := r.URL.Query()
		for key := range queryUrl {
			sort.Strings(queryUrl[key])
		}
		canonicalQueryString := strings.ReplaceAll(queryUrl.Encode(), "+", "%20")

		// Canonical headers
		// Note: Go's net/http moves Host header from r.Header to r.Host
		headers := strings.Split(signedHeaders, ";")
		sort.Strings(headers)

		canonicalHeaders := ""
		for _, header := range headers {
			var value string
			if header == "host" {
				// Host header is in r.Host, not r.Header in net/http
				value = r.Host
			} else {
				value = r.Header.Get(header)
			}
			canonicalHeaders += fmt.Sprintf("%s:%s\n", header, strings.TrimSpace(value))
		}

		// Payload hash — use the client-provided hash from X-Amz-Content-SHA256 when
		// it contains a precomputed hex digest. This avoids buffering the entire
		// request body into memory (critical for large object uploads).
		var payloadHash string
		payloadEncoding := r.Header.Get("X-Amz-Content-Sha256")
		switch {
		case payloadEncoding == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || payloadEncoding == "UNSIGNED-PAYLOAD":
			payloadHash = payloadEncoding
		case isHexSHA256(payloadEncoding):
			// Client sent the precomputed hash — use it directly. The signature
			// verification below guarantees integrity: if the client lied about
			// the hash, the signature won't match.
			payloadHash = payloadEncoding
		default:
			// Read body for signature verification
			body, err := io.ReadAll(r.Body)
			if err != nil {
				s.writeS3Error(w, r, http.StatusInternalServerError, "InternalError", "Failed to read request body")
				return
			}
			// Put body back for handlers using bytes.NewReader (preserves binary data)
			r.Body = io.NopCloser(bytes.NewReader(body))
			payloadHash = auth.HashSHA256(string(body))
		}

		canonicalRequest := fmt.Sprintf(
			"%s\n%s\n%s\n%s\n%s\n%s",
			method,
			canonicalURI,
			canonicalQueryString,
			canonicalHeaders,
			signedHeaders,
			payloadHash,
		)

		hashedCanonicalRequest := auth.HashSHA256(canonicalRequest)

		scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, s.config.Region, svc)

		stringToSign := fmt.Sprintf(
			"AWS4-HMAC-SHA256\n%s\n%s\n%s",
			amzDate,
			scope,
			hashedCanonicalRequest,
		)

		signingKey := auth.GetSigningKey(secretKey, date, region, svc)
		expectedSig := auth.HmacSHA256Hex(signingKey, stringToSign)

		if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(signature)) != 1 {
			slog.Debug("Invalid signature", "expected", expectedSig, "actual", signature)
			s.writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "The request signature does not match")
			return
		}

		// Check IAM policy authorization for NATS-sourced credentials
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

		// Store authenticated user info in context
		ctx := context.WithValue(r.Context(), ContextKeyAccessKeyID, accessKey)
		ctx = context.WithValue(ctx, ContextKeyAccountID, credResult.AccountID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
		NextProtos: []string{"h2", "http/1.1"},
		MinVersion: tls.VersionTLS12,
		// Optimized cipher suites for performance
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		// Session resumption for faster reconnects
		SessionTicketsDisabled: false,
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

// isHexSHA256 returns true if s is exactly 64 lowercase hex characters (a SHA-256 digest).
func isHexSHA256(s string) bool {
	if len(s) != hex.EncodedLen(32) {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
