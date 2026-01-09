package backend

import "fmt"

// S3ErrorCode represents standardized S3 error codes
type S3ErrorCode string

const (
	ErrNoSuchBucket           S3ErrorCode = "NoSuchBucket"
	ErrNoSuchKey              S3ErrorCode = "NoSuchKey"
	ErrNoSuchUpload           S3ErrorCode = "NoSuchUpload"
	ErrInvalidKey             S3ErrorCode = "InvalidKey"
	ErrInvalidPart            S3ErrorCode = "InvalidPart"
	ErrInvalidPartOrder       S3ErrorCode = "InvalidPartOrder"
	ErrAccessDenied           S3ErrorCode = "AccessDenied"
	ErrInternalError          S3ErrorCode = "InternalError"
	ErrEntityTooSmall         S3ErrorCode = "EntityTooSmall"
	ErrInvalidRange           S3ErrorCode = "InvalidRange"
	ErrBucketNotEmpty         S3ErrorCode = "BucketNotEmpty"
	ErrBucketAlreadyExists    S3ErrorCode = "BucketAlreadyExists"
	ErrBucketAlreadyOwnedByYou S3ErrorCode = "BucketAlreadyOwnedByYou"
	ErrInvalidBucketName      S3ErrorCode = "InvalidBucketName"
	ErrMissingParameter       S3ErrorCode = "MissingParameter"
	ErrChecksumMismatch       S3ErrorCode = "XAmzContentChecksumMismatch"
)

// S3Error represents a typed S3 error with code and message
type S3Error struct {
	Code       S3ErrorCode
	Message    string
	StatusCode int
	Resource   string
}

// Error implements the error interface
func (e *S3Error) Error() string {
	if e.Resource != "" {
		return fmt.Sprintf("%s: %s (resource: %s)", e.Code, e.Message, e.Resource)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Is implements error comparison for errors.Is()
func (e *S3Error) Is(target error) bool {
	t, ok := target.(*S3Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// Predefined errors for common cases
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

	ErrInvalidKeyError = &S3Error{
		Code:       ErrInvalidKey,
		Message:    "The specified key is not valid",
		StatusCode: 400,
	}

	ErrInvalidPartError = &S3Error{
		Code:       ErrInvalidPart,
		Message:    "One or more of the specified parts could not be found",
		StatusCode: 400,
	}

	ErrAccessDeniedError = &S3Error{
		Code:       ErrAccessDenied,
		Message:    "Access Denied",
		StatusCode: 403,
	}

	ErrInternalServerError = &S3Error{
		Code:       ErrInternalError,
		Message:    "We encountered an internal error. Please try again.",
		StatusCode: 500,
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

	ErrInvalidBucketNameError = &S3Error{
		Code:       ErrInvalidBucketName,
		Message:    "The specified bucket is not valid.",
		StatusCode: 400,
	}

	ErrBucketNotEmptyError = &S3Error{
		Code:       ErrBucketNotEmpty,
		Message:    "The bucket you tried to delete is not empty.",
		StatusCode: 409,
	}
)

// NewS3Error creates a new S3Error with the given code
func NewS3Error(code S3ErrorCode, message string, statusCode int) *S3Error {
	return &S3Error{
		Code:       code,
		Message:    message,
		StatusCode: statusCode,
	}
}

// WithResource adds a resource path to an S3Error
func (e *S3Error) WithResource(resource string) *S3Error {
	return &S3Error{
		Code:       e.Code,
		Message:    e.Message,
		StatusCode: e.StatusCode,
		Resource:   resource,
	}
}

// IsS3Error checks if an error is an S3Error and returns it
func IsS3Error(err error) (*S3Error, bool) {
	if s3err, ok := err.(*S3Error); ok {
		return s3err, true
	}
	return nil, false
}

// GetHTTPStatus returns the HTTP status code for an error
func GetHTTPStatus(err error) int {
	if s3err, ok := IsS3Error(err); ok {
		return s3err.StatusCode
	}
	return 500
}
