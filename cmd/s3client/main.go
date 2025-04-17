package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Options for the client
type ClientOptions struct {
	Endpoint     string
	Region       string
	Bucket       string
	Method       string
	Path         string
	Body         string
	BodyFile     string
	AccessKey    string
	SecretKey    string
	OutputFile   string
	SkipTLSCheck bool
}

func main() {
	// Parse command line options
	opts := parseFlags()

	// Read body from file if specified
	var body string
	if opts.BodyFile != "" {
		data, err := os.ReadFile(opts.BodyFile)
		if err != nil {
			fmt.Printf("Error reading body file: %v\n", err)
			os.Exit(1)
		}
		body = string(data)
	} else {
		body = opts.Body
	}

	// Construct URL
	url := opts.Endpoint
	if !strings.HasSuffix(url, "/") {
		url += "/"
	}

	// Add bucket and path to URL if provided
	if opts.Bucket != "" {
		url += opts.Bucket

		if opts.Path != "" {
			if !strings.HasPrefix(opts.Path, "/") {
				url += "/"
			}
			url += opts.Path
		}
	}

	// Create HTTP client (skip TLS verification if requested)
	client := &http.Client{}
	if opts.SkipTLSCheck {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{Transport: tr}
	}

	// Create request
	req, err := http.NewRequest(opts.Method, url, bytes.NewReader([]byte(body)))
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		os.Exit(1)
	}

	// Set headers
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	hostname := getHostnameFromURL(opts.Endpoint)

	req.Header.Set("Host", hostname)
	req.Header.Set("X-Amz-Date", timestamp)

	// Add authentication if credentials are provided
	if opts.AccessKey != "" && opts.SecretKey != "" {
		fmt.Println("Adding authentication header")
		authHeader := generateAuthHeader(
			opts.AccessKey,
			opts.SecretKey,
			opts.Method,
			"/"+opts.Bucket+"/"+strings.TrimPrefix(opts.Path, "/"),
			body,
			timestamp,
			hostname,
			opts.Region,
			"s3",
		)
		req.Header.Set("Authorization", authHeader)
	}

	// Send request
	fmt.Printf("Sending %s request to %s\n", opts.Method, url)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// Print response headers
	fmt.Printf("Response status: %s\n", resp.Status)
	fmt.Println("Response headers:")
	for name, values := range resp.Header {
		for _, value := range values {
			fmt.Printf("  %s: %s\n", name, value)
		}
	}

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		os.Exit(1)
	}

	// Write to output file or stdout
	if opts.OutputFile != "" {
		err = os.WriteFile(opts.OutputFile, responseBody, 0644)
		if err != nil {
			fmt.Printf("Error writing to output file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Response written to %s (%d bytes)\n", opts.OutputFile, len(responseBody))
	} else {
		fmt.Println("\nResponse body:")
		fmt.Println(string(responseBody))
	}
}

// Parse command line flags
func parseFlags() ClientOptions {
	opts := ClientOptions{}

	flag.StringVar(&opts.Endpoint, "endpoint", "https://localhost:443", "S3 endpoint URL")
	flag.StringVar(&opts.Region, "region", "us-east-1", "AWS region")
	flag.StringVar(&opts.Bucket, "bucket", "", "S3 bucket name")
	flag.StringVar(&opts.Method, "method", "GET", "HTTP method (GET, PUT, HEAD, DELETE)")
	flag.StringVar(&opts.Path, "path", "", "Path within bucket")
	flag.StringVar(&opts.Body, "body", "", "Request body (for PUT)")
	flag.StringVar(&opts.BodyFile, "body-file", "", "File to read request body from")
	flag.StringVar(&opts.AccessKey, "access-key", "", "AWS access key")
	flag.StringVar(&opts.SecretKey, "secret-key", "", "AWS secret key")
	flag.StringVar(&opts.OutputFile, "output", "", "File to write response to")
	flag.BoolVar(&opts.SkipTLSCheck, "skip-tls-check", false, "Skip TLS certificate verification")

	flag.Parse()

	return opts
}

// Extract hostname from URL
func getHostnameFromURL(url string) string {
	// Strip protocol
	hostname := url
	if strings.Contains(hostname, "://") {
		hostname = strings.Split(hostname, "://")[1]
	}

	// Strip port and path
	if strings.Contains(hostname, ":") {
		hostname = strings.Split(hostname, ":")[0]
	}
	if strings.Contains(hostname, "/") {
		hostname = strings.Split(hostname, "/")[0]
	}

	return hostname
}

// Calculate SHA256 hash of a string
func hashSHA256(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// Calculate HMAC-SHA256
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// Calculate HMAC-SHA256 and encode as hex
func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

// Get AWS SigV4 signing key
func getSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

// Generate AWS SigV4 authorization header
func generateAuthHeader(accessKey, secretKey, method, path, body, timestamp, host, region, service string) string {
	// Get the date portion of the timestamp
	date := timestamp[:8]

	// Create canonical request
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", host, timestamp)
	signedHeaders := "host;x-amz-date"
	payloadHash := hashSHA256(body)

	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n\n%s\n%s\n%s",
		method,
		path,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	// Hash the canonical request
	hashedCanonicalRequest := hashSHA256(canonicalRequest)

	// Create string to sign
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		timestamp,
		scope,
		hashedCanonicalRequest,
	)

	// Derive signing key
	signingKey := getSigningKey(secretKey, date, region, service)

	// Calculate signature
	signature := hmacSHA256Hex(signingKey, stringToSign)

	// Create authorization header
	return fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
		accessKey,
		date,
		region,
		service,
		signedHeaders,
		signature,
	)
}
