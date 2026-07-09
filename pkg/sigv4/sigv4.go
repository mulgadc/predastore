package sigv4

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	amzTimeFormat = "20060102T150405Z"
	amzDateFormat = "20060102"

	MaxClockSkew  = 15 * time.Minute
	MaxPresignAge = 7 * 24 * time.Hour
)

type Algorithm string

const (
	AlgorithmV4  Algorithm = "AWS4-HMAC-SHA256"
	AlgorithmV4a Algorithm = "AWS4-ECDSA-P256-SHA256"
)

type ParsedRequest struct {
	*http.Request

	AccessKeyID   string
	Timestamp     time.Time
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
	Content       string
}

// TODO: Standardize errors.

func Parse(req *http.Request, region string, service string) (*ParsedRequest, error) {
	parsed := &ParsedRequest{
		Request: req,
	}

	// Get Authorization header.
	authHdr := req.Header.Get("Authorization")
	if authHdr == "" {
		return nil, fmt.Errorf("no Authorization header present")
	}

	// Parse Authorization algorithm.
	algo, remainder, found := strings.Cut(authHdr, " ")
	if !found {
		return nil, fmt.Errorf("malformed Authorization header: one and only one ' ' (space) required")
	}
	// TODO: When SigV4a support is added, allow AlgorithmV4a to pass this check.
	if algo != string(AlgorithmV4) {
		return nil, fmt.Errorf("unsupported Authorization type")
	}

	// Get content hash (or signing scheme).
	parsed.Content = req.Header.Get("X-Amz-Content-Sha256")
	if parsed.Content == "" {
		return nil, fmt.Errorf("missing required header for this request: x-amz-content-sha256")
	}

	// Parse request timestamp, and save raw date header value (needed for exact signature calculation).
	var dateHdr string
	if dateHdr = req.Header.Get("X-Amz-Date"); dateHdr != "" {
		t, err := time.Parse(amzTimeFormat, dateHdr)
		if err != nil {
			// If X-Amz-Date is present (and not empty), but parsing fails, return an error without falling
			// through to the Date header.
			return nil, fmt.Errorf("sigV4 authentication requires a valid X-Amz-Date or Date header")
		}
		parsed.Timestamp = t
	} else {
		// If X-Amz-Date is missing/empty, attempt to use the Date header.
		dateHdr = req.Header.Get("Date")
		t, err := http.ParseTime(dateHdr)
		if err != nil {
			return nil, fmt.Errorf("sigV4 authentication requires a valid X-Amz-Date or Date header")
		}
		parsed.Timestamp = t
	}
	// Normalize to UTC for easy downstream comparison.
	parsed.Timestamp = parsed.Timestamp.UTC()

	// Check current time - timestamp < MaxClockSkew.
	if skew := time.Since(parsed.Timestamp).Abs(); skew > MaxClockSkew {
		return nil, fmt.Errorf("the difference between the request time and the current time is too large")
	}

	// Separate Authorization key=value components, and check we have the right quantity.
	authHdrParts := strings.Split(remainder, ",")
	if len(authHdrParts) != 3 {
		return nil, fmt.Errorf("malformed Authorization header: incorrect number of components provided")
	}

	// Check we have the correct fields, and get their value strings (without key prefix).
	cred, credFound := strings.CutPrefix(strings.TrimSpace(authHdrParts[0]), "Credential=")
	hdrs, hdrsFound := strings.CutPrefix(strings.TrimSpace(authHdrParts[1]), "SignedHeaders=")
	sig, sigFound := strings.CutPrefix(strings.TrimSpace(authHdrParts[2]), "Signature=")
	if !credFound || !hdrsFound || !sigFound {
		return nil, fmt.Errorf("malformed Authorization header: required components are missing or in an incorrect order")
	}

	// Split the Credential string into it's subcomponents, and check that we have the right amount.
	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return nil, fmt.Errorf("malformed Authorization header: expected Credential to be in the format \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\"")
	}

	// Confirm the Credential date is in the appropriate format, and that it matches the date
	// component of the request timestamp.
	if _, err := time.Parse(amzDateFormat, credParts[1]); err != nil {
		return nil, fmt.Errorf("malformed Authorization header: the second Credential element must be a date in the format \"YYYYMMDD\"")
	} else if credParts[1] != parsed.Timestamp.Format(amzDateFormat) {
		return nil, fmt.Errorf("malformed Authorization header: date does not match X-Amz-Date (or Date, if X-Amz-Date is not set)")
	}

	// Check that Credential region and service match the endpoint's expected values.
	parsed.Region = credParts[2]
	parsed.Service = credParts[3]
	if parsed.Region != region {
		return nil, fmt.Errorf("malformed Authorization header: incorrect region \"%s\"; expected \"%s\"", parsed.Region, region)
	} else if parsed.Service != service {
		return nil, fmt.Errorf("malformed Authorization header: incorrect service \"%s\"; expected \"%s\"", parsed.Service, service)
	}

	// Check that Credential has the correct terminal value.
	if credParts[4] != "aws4_request" {
		return nil, fmt.Errorf("malformed Authorization header: terminal value; expected \"aws4_request\"")
	}

	// Assign remaining fields to parsed request envelope.
	parsed.AccessKeyID = credParts[0]
	parsed.SignedHeaders = strings.Split(hdrs, ";")
	parsed.Signature = sig

	return parsed, nil
}

func (req *ParsedRequest) Verify(secretAccessKey string) error {
	panic("Verify not implemented")
}
