package predastore_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// sdkClient returns an aws-sdk-go-v2 S3 client on the host's gate. It does not
// retry, so each call is exactly one request and a 412 is never hidden.
func sdkClient(t *testing.T) *s3.Client {
	t.Helper()
	base, httpClient := startAuthedHost(t)
	client := s3.New(s3.Options{
		Region:       attrsRegion,
		BaseEndpoint: aws.String(base),
		UsePathStyle: true,
		HTTPClient:   httpClient,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: attrsAccessKey, SecretAccessKey: attrsSecretKey}, nil
		}),
		Retryer: aws.NopRetryer{},
	})

	// The gate binds after /readyz answers, so the standard retryer's backoff
	// covers the window before its listener exists.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(attrsBucket)}, func(o *s3.Options) {
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = 100
			so.MaxBackoff = 200 * time.Millisecond
		})
	})
	require.NoError(t, err)
	return client
}

// requirePreconditionFailed checks the error the way an SDK caller does: a
// smithy API error with code PreconditionFailed on an HTTP 412 response.
func requirePreconditionFailed(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "PreconditionFailed", apiErr.ErrorCode())
	var respErr *awshttp.ResponseError
	require.ErrorAs(t, err, &respErr)
	require.Equal(t, http.StatusPreconditionFailed, respErr.HTTPStatusCode())
	require.NotEmpty(t, respErr.ServiceRequestID())
}

func getBody(t *testing.T, client *s3.Client, key string) (string, string) {
	t.Helper()
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(attrsBucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	return string(body), aws.ToString(out.ETag)
}

func put(ctx context.Context, client *s3.Client, key, body string, opt func(*s3.PutObjectInput)) (*s3.PutObjectOutput, error) {
	in := &s3.PutObjectInput{Bucket: aws.String(attrsBucket), Key: aws.String(key), Body: strings.NewReader(body)}
	opt(in)
	return client.PutObject(ctx, in)
}

// If-None-Match: * through the SDK creates an absent key, and a second create
// of the same key is a typed 412 that leaves the first body in place.
func TestSDKPutObjectIfNoneMatch(t *testing.T) {
	client := sdkClient(t)
	ctx := context.Background()
	const key = "lock/create-only.tflock"
	ifNoneMatch := func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") }

	first, err := put(ctx, client, key, "first", ifNoneMatch)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(first.ETag))

	_, err = put(ctx, client, key, "second", ifNoneMatch)
	requirePreconditionFailed(t, err)

	body, etag := getBody(t, client, key)
	require.Equal(t, "first", body)
	require.Equal(t, aws.ToString(first.ETag), etag)
}

// If-Match through the SDK refuses a stale ETag with a typed 412 and accepts
// the current one, which returns the new object's ETag.
func TestSDKPutObjectIfMatch(t *testing.T) {
	client := sdkClient(t)
	ctx := context.Background()
	const key = "state/default.tfstate"
	none := func(*s3.PutObjectInput) {}

	// If-Match on an absent key is a 404 NoSuchKey, not a 412.
	_, err := put(ctx, client, key, "v0", func(in *s3.PutObjectInput) { in.IfMatch = aws.String(`"0123456789abcdef0123456789abcdef"`) })
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchKey", apiErr.ErrorCode())
	var respErr *awshttp.ResponseError
	require.ErrorAs(t, err, &respErr)
	require.Equal(t, http.StatusNotFound, respErr.HTTPStatusCode())

	v1, err := put(ctx, client, key, "v1", none)
	require.NoError(t, err)
	v2, err := put(ctx, client, key, "v2", none)
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(v1.ETag), aws.ToString(v2.ETag))

	_, err = put(ctx, client, key, "stale", func(in *s3.PutObjectInput) { in.IfMatch = v1.ETag })
	requirePreconditionFailed(t, err)
	body, _ := getBody(t, client, key)
	require.Equal(t, "v2", body)

	v3, err := put(ctx, client, key, "v3", func(in *s3.PutObjectInput) { in.IfMatch = v2.ETag })
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(v2.ETag), aws.ToString(v3.ETag))
	body, etag := getBody(t, client, key)
	require.Equal(t, "v3", body)
	require.Equal(t, aws.ToString(v3.ETag), etag)
}
