package handlers

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/pkg/otelsetup"
	"github.com/mulgadc/predastore/pkg/sigv4"
)

// RespondSigV4Error maps a parse/verify sentinel or a credential lookup
// error to the matching S3 error response.
func RespondSigV4Error(w http.ResponseWriter, r *http.Request, claimedKey string, err, lookupErr error) {
	if lookupErr != nil {
		if errors.Is(lookupErr, auth.ErrKeyNotFound) {
			slog.WarnContext(r.Context(), "Unknown access key", "accessKeyID", claimedKey, "remoteAddr", r.RemoteAddr)
			WriteS3Error(w, r, http.StatusForbidden, "InvalidAccessKeyId",
				"The AWS Access Key Id you provided does not exist in our records")
			return
		}
		slog.ErrorContext(r.Context(), "Credential lookup infrastructure error",
			"accessKeyID", claimedKey, "error", lookupErr, "remoteAddr", r.RemoteAddr)
		WriteS3Error(w, r, http.StatusInternalServerError, "InternalError",
			"An internal error occurred while validating credentials")
		return
	}

	switch {
	case errors.Is(err, sigv4.ErrMissingAuthentication):
		WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Missing Authorization header")
	case errors.Is(err, sigv4.ErrUnsignedHeader):
		WriteS3Error(w, r, http.StatusBadRequest, "AuthorizationHeaderMalformed", err.Error())
	case errors.Is(err, sigv4.ErrUnsupportedAlgorithm):
		WriteS3Error(w, r, http.StatusBadRequest, "AuthorizationHeaderMalformed", "Invalid Authorization header format")
	case errors.Is(err, sigv4.ErrMalformedAuthorization):
		WriteS3Error(w, r, http.StatusBadRequest, "AuthorizationHeaderMalformed", err.Error())
	case errors.Is(err, sigv4.ErrMalformedPresignedURL):
		WriteS3Error(w, r, http.StatusBadRequest, "AuthorizationQueryParametersError", err.Error())
	case errors.Is(err, sigv4.ErrRequestTimeInvalid):
		WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Missing required header: X-Amz-Date")
	case errors.Is(err, sigv4.ErrMissingContentSHA256):
		WriteS3Error(w, r, http.StatusBadRequest, "InvalidRequest", "Missing required header: X-Amz-Content-Sha256")
	case errors.Is(err, sigv4.ErrRequestTimeTooSkewed):
		slog.DebugContext(r.Context(), "Request timestamp outside allowed skew", "timestamp", r.Header.Get("X-Amz-Date"))
		WriteS3Error(w, r, http.StatusForbidden, "RequestTimeTooSkewed",
			"The difference between the request time and the current time is too large")
	case errors.Is(err, sigv4.ErrPresignedURLExpired):
		slog.DebugContext(r.Context(), "Presigned URL expired or not yet valid", "timestamp", r.URL.Query().Get("X-Amz-Date"))
		WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", "Request has expired")
	case errors.Is(err, sigv4.ErrSignatureMismatch):
		slog.WarnContext(r.Context(), "SigV4 signature mismatch",
			"accessKeyID", claimedKey,
			"method", r.Method,
			"path", r.URL.Path,
			"host", r.Host,
			"amzDate", r.Header.Get("X-Amz-Date"),
			"payloadHashHeader", r.Header.Get("X-Amz-Content-Sha256"),
			"contentLength", r.Header.Get("Content-Length"),
			"userAgent", r.Header.Get("User-Agent"),
			"proto", r.Proto,
			"remoteAddr", r.RemoteAddr,
		)
		WriteS3Error(w, r, http.StatusForbidden, "SignatureDoesNotMatch",
			"The request signature we calculated does not match the signature you provided. Check your key and signing method.")
	default:
		slog.WarnContext(r.Context(), "Unexpected SigV4 verification error", "error", err, "accessKeyID", claimedKey)
		WriteS3Error(w, r, http.StatusForbidden, "AccessDenied", err.Error())
	}
}

// WriteS3Error writes an S3 error response.
func WriteS3Error(w http.ResponseWriter, r *http.Request, statusCode int, code, message string) {
	s3error := S3Error{
		Code:      code,
		Message:   message,
		RequestId: uuid.NewString(),
		HostId:    r.Host,
	}
	otelsetup.SetRequestErrorCode(r.Context(), code)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	if err := xml.NewEncoder(w).Encode(s3error); err != nil {
		slog.DebugContext(r.Context(), "failed to encode XML error response", "error", err)
	}
}

// writeXML writes an XML response.
func writeXML(w http.ResponseWriter, statusCode int, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	return xml.NewEncoder(w).Encode(v)
}

// HandleError converts an operation's error to an S3 error response.
func HandleError(w http.ResponseWriter, r *http.Request, err error) {
	statusCode := http.StatusInternalServerError
	var s3error S3Error

	if opErr, ok := model.IsS3Error(err); ok {
		statusCode = opErr.StatusCode
		s3error.Code = string(opErr.Code)
		s3error.Message = opErr.Message
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
	otelsetup.SetRequestErrorCode(r.Context(), s3error.Code)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	if err := xml.NewEncoder(w).Encode(s3error); err != nil {
		slog.DebugContext(r.Context(), "failed to encode XML error response", "error", err)
	}
}
