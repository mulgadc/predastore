package model

import (
	"errors"
	"fmt"
	"net/http"
)

// S3ErrorCode represents standardized S3 error codes.
type S3ErrorCode string

const (
	ErrNoSuchBucket            S3ErrorCode = "NoSuchBucket"
	ErrNoSuchKey               S3ErrorCode = "NoSuchKey"
	ErrNoSuchUpload            S3ErrorCode = "NoSuchUpload"
	ErrInvalidKey              S3ErrorCode = "InvalidKey"
	ErrInvalidPart             S3ErrorCode = "InvalidPart"
	ErrInvalidPartOrder        S3ErrorCode = "InvalidPartOrder"
	ErrAccessDenied            S3ErrorCode = "AccessDenied"
	ErrInternalError           S3ErrorCode = "InternalError"
	ErrEntityTooSmall          S3ErrorCode = "EntityTooSmall"
	ErrEntityTooLarge          S3ErrorCode = "EntityTooLarge"
	ErrInvalidRange            S3ErrorCode = "InvalidRange"
	ErrBucketNotEmpty          S3ErrorCode = "BucketNotEmpty"
	ErrBucketAlreadyExists     S3ErrorCode = "BucketAlreadyExists"
	ErrBucketAlreadyOwnedByYou S3ErrorCode = "BucketAlreadyOwnedByYou"
	ErrInvalidBucketName       S3ErrorCode = "InvalidBucketName"
	ErrMissingParameter        S3ErrorCode = "MissingParameter"
	ErrChecksumMismatch        S3ErrorCode = "XAmzContentChecksumMismatch"
	ErrContentSHA256Mismatch   S3ErrorCode = "XAmzContentSHA256Mismatch"
	ErrInsufficientStorage     S3ErrorCode = "InsufficientStorage"
	ErrMissingContentLength    S3ErrorCode = "MissingContentLength"
	ErrInvalidArgument         S3ErrorCode = "InvalidArgument"
	ErrSignatureDoesNotMatch   S3ErrorCode = "SignatureDoesNotMatch"
	ErrMalformedChunkedBody    S3ErrorCode = "InvalidRequest"
	ErrMalformedXML            S3ErrorCode = "MalformedXML"
)

// S3Error represents a typed S3 error with code and message.
type S3Error struct {
	Code       S3ErrorCode
	Message    string
	StatusCode int
	Resource   string
}

// Error implements the error interface.
func (e *S3Error) Error() string {
	if e.Resource != "" {
		return fmt.Sprintf("%s: %s (resource: %s)", e.Code, e.Message, e.Resource)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Is implements error comparison for errors.Is().
func (e *S3Error) Is(target error) bool {
	t, ok := target.(*S3Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// Predefined errors for common cases.
var (
	ErrNoSuchBucketError = &S3Error{
		Code:       ErrNoSuchBucket,
		Message:    "The specified bucket does not exist",
		StatusCode: 404,
	}

	ErrNoSuchKeyError = &S3Error{
		Code:       ErrNoSuchKey,
		Message:    "The specified key does not exist",
		StatusCode: 404,
	}

	ErrNoSuchUploadError = &S3Error{
		Code:       ErrNoSuchUpload,
		Message:    "The specified upload does not exist",
		StatusCode: 404,
	}

	ErrAccessDeniedError = &S3Error{
		Code:       ErrAccessDenied,
		Message:    "Access Denied",
		StatusCode: 403,
	}

	ErrInvalidRangeError = &S3Error{
		Code:       ErrInvalidRange,
		Message:    "The requested range is not satisfiable",
		StatusCode: 416,
	}

	ErrBucketAlreadyExistsError = &S3Error{
		Code:       ErrBucketAlreadyExists,
		Message:    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		StatusCode: 409,
	}

	ErrBucketAlreadyOwnedByYouError = &S3Error{
		Code:       ErrBucketAlreadyOwnedByYou,
		Message:    "Your previous request to create the named bucket succeeded and you already own it.",
		StatusCode: 409,
	}

	ErrBucketNotEmptyError = &S3Error{
		Code:       ErrBucketNotEmpty,
		Message:    "The bucket you tried to delete is not empty.",
		StatusCode: 409,
	}

	// ErrInsufficientStorageError is returned when the store's free-space
	// watermark rejects a write; 507 lets clients distinguish this from a
	// transient 500.
	ErrInsufficientStorageError = &S3Error{
		Code:       ErrInsufficientStorage,
		Message:    "The storage pool is at capacity and cannot accept new writes",
		StatusCode: http.StatusInsufficientStorage,
	}

	// ErrContentSHA256MismatchError is returned when the body of a signed write
	// does not hash to the digest the client signed in x-amz-content-sha256.
	ErrContentSHA256MismatchError = &S3Error{
		Code:       ErrContentSHA256Mismatch,
		Message:    "The provided 'x-amz-content-sha256' header does not match what was computed",
		StatusCode: http.StatusBadRequest,
	}

	// ErrMissingContentLengthError is returned when a write carries a body of
	// undeclared length. The erasure coder has to know the size before it can
	// split, and S3 requires the header on every PUT, so there is nothing to
	// fall back to.
	ErrMissingContentLengthError = &S3Error{
		Code:       ErrMissingContentLength,
		Message:    "You must provide the Content-Length HTTP header",
		StatusCode: http.StatusLengthRequired,
	}

	// ErrSignatureDoesNotMatchError is returned when a chunk of an aws-chunked
	// body does not continue the signature chain seeded by the request
	// signature, which means those bytes are not the bytes the client signed.
	ErrSignatureDoesNotMatchError = &S3Error{
		Code:       ErrSignatureDoesNotMatch,
		Message:    "The request signature we calculated does not match the signature you provided",
		StatusCode: http.StatusForbidden,
	}

	// ErrMalformedChunkedBodyError is returned when an aws-chunked body's
	// framing does not parse. The alternative is storing the framing as object
	// data, which is what a missed decode does.
	ErrMalformedChunkedBodyError = &S3Error{
		Code:       ErrMalformedChunkedBody,
		Message:    "The chunked upload framing in the request body is malformed",
		StatusCode: http.StatusBadRequest,
	}

	// ErrChecksumMismatchError is returned when a body's trailing checksum does
	// not match the one computed as it streamed.
	ErrChecksumMismatchError = &S3Error{
		Code:       ErrChecksumMismatch,
		Message:    "The checksum the client sent does not match what was computed",
		StatusCode: http.StatusBadRequest,
	}
)

// NewS3Error creates a new S3Error with the given code.
func NewS3Error(code S3ErrorCode, message string, statusCode int) *S3Error {
	return &S3Error{
		Code:       code,
		Message:    message,
		StatusCode: statusCode,
	}
}

// WithResource adds a resource path to an S3Error.
func (e *S3Error) WithResource(resource string) *S3Error {
	return &S3Error{
		Code:       e.Code,
		Message:    e.Message,
		StatusCode: e.StatusCode,
		Resource:   resource,
	}
}

// IsS3Error checks if an error is an S3Error and returns it.
func IsS3Error(err error) (*S3Error, bool) {
	if s3err, ok := errors.AsType[*S3Error](err); ok {
		return s3err, true
	}
	return nil, false
}
