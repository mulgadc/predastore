package s3db

import (
	"net/http"
	"time"

	"github.com/mulgadc/predastore/auth"
)

const (
	// DefaultRegion is the default AWS region for s3db
	DefaultRegion = "us-east-1"

	// DefaultService is the service name for s3db signing
	DefaultService = "s3db"
)

// SignRequest signs an HTTP request using AWS Signature V4
// Uses the auth.GenerateAuthHeaderReq function for consistency with S3 API
func SignRequest(req *http.Request, accessKey, secretKey, region, service string) error {
	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	return auth.GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service, req)
}
