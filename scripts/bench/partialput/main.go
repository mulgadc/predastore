// Command partialput opens a PUT that declares a body length and then stops
// sending part-way through, which is the shape of a client that dies mid-upload:
// a killed guest, a dropped NAT entry, a half-open connection. The gate sees a
// request whose body simply stops arriving and whose peer never closes.
//
// No S3 client can express this. They all finish the body or abort the request,
// and both are the cases that already work. The stall is the one that does not,
// so it needs a client that will hold the connection open and send nothing.
//
//	go run ./scripts/bench/partialput -endpoint https://127.0.0.1:8443 \
//	    -bucket b -key k -declare 1048576 -send 524288 -hold 90s
//
// It prints one key=value line describing how the request ended, and exits 0
// whenever the outcome was observed — "the server never replied" is the finding
// the caller is testing for, not an error in observing it. Exit 2 is reserved
// for being unable to run the probe at all.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// unsignedPayload lets the request be signed before the body exists. A stalled
// upload has no payload hash to sign because the payload is never finished.
const unsignedPayload = "UNSIGNED-PAYLOAD"

// stallingReader yields send bytes and then blocks until deadline, leaving the
// connection open and idle. Returning io.EOF instead would be a truncated but
// well-behaved client, which is a different test and one that already works.
//
// The block is shared by every later Read, not restarted per call, so the total
// stall is the one the caller asked for however many times the transport reads.
type stallingReader struct {
	send      int64
	sent      int64
	rate      int64
	release   <-chan struct{}
	stalledAt chan<- time.Time
	signalled bool
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if r.sent < r.send {
		n := int64(len(p))
		if remaining := r.send - r.sent; n > remaining {
			n = remaining
		}
		// Pacing keeps the upload genuinely in flight for a known duration, so
		// a caller that kills this process mid-transfer can be sure it did.
		// Unthrottled, a loopback body of any size is gone in under a second.
		if r.rate > 0 {
			time.Sleep(time.Duration(float64(n) / float64(r.rate) * float64(time.Second)))
		}
		for i := range p[:n] {
			p[i] = 'x'
		}
		r.sent += n
		return int(n), nil
	}

	if !r.signalled {
		r.signalled = true
		select {
		case r.stalledAt <- time.Now():
		default:
		}
	}

	// The body is declared longer than this, so the gate is still waiting on
	// bytes that will not arrive.
	<-r.release
	return 0, io.EOF
}

func main() {
	endpoint := flag.String("endpoint", "", "gate endpoint, e.g. https://127.0.0.1:8443")
	bucket := flag.String("bucket", "", "bucket name")
	key := flag.String("key", "", "object key")
	declare := flag.Int64("declare", 1<<20, "Content-Length to declare")
	send := flag.Int64("send", 0, "bytes to actually send before stalling")
	hold := flag.Duration("hold", 90*time.Second, "how long to stall before giving up")
	rate := flag.Int64("rate", 0, "bytes per second to send at, 0 for as fast as the link allows")
	region := flag.String("region", "ap-southeast-2", "signing region")
	accessKey := flag.String("access-key", "", "access key id")
	secretKey := flag.String("secret-key", "", "secret access key")
	useHTTP2 := flag.Bool("http2", false, "offer h2 over ALPN instead of forcing HTTP/1.1")
	flag.Parse()

	if *endpoint == "" || *bucket == "" || *key == "" || *accessKey == "" || *secretKey == "" {
		fmt.Fprintln(os.Stderr, "usage: partialput -endpoint <url> -bucket <b> -key <k> -access-key <id> -secret-key <secret> [-declare n] [-send n] [-hold d]")
		os.Exit(2)
	}
	if *send >= *declare {
		fmt.Fprintln(os.Stderr, "-send must be less than -declare, or the body is complete and nothing stalls")
		os.Exit(2)
	}

	stalled := make(chan time.Time, 1)

	// Closed rather than a timer channel: every Read after the stall waits on
	// this, and a timer channel only ever delivers once.
	release := make(chan struct{})
	time.AfterFunc(*hold, func() { close(release) })
	body := &stallingReader{send: *send, rate: *rate, release: release, stalledAt: stalled}

	url := fmt.Sprintf("%s/%s/%s", *endpoint, *bucket, *key)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build request: %v\n", err)
		os.Exit(2)
	}
	req.ContentLength = *declare
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	creds := aws.Credentials{AccessKeyID: *accessKey, SecretAccessKey: *secretKey}
	if err := v4.NewSigner().SignHTTP(req.Context(), creds, req, unsignedPayload, "s3", *region, time.Now()); err != nil {
		fmt.Fprintf(os.Stderr, "sign request: %v\n", err)
		os.Exit(2)
	}

	// The harness runs against a development CA and the point of the probe is
	// the body, not the chain.
	//
	// The protocol is selectable because it may be the whole story: a stalled
	// h2 stream is a flow-control state on a shared connection, where a stalled
	// HTTP/1.1 body is a socket the server can time out on its own terms.
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // G402: local harness against a development CA.
		ForceAttemptHTTP2: *useHTTP2,
	}
	if !*useHTTP2 {
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	client := &http.Client{Transport: transport}

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	stallAt := time.Duration(-1)
	select {
	case t := <-stalled:
		stallAt = t.Sub(start)
	default:
	}

	// A server that abandons a stalled body is the fixed behaviour; one that
	// waits out the hold is the bug. Both are reported the same way, because
	// which one happened is the caller's assertion to make.
	switch {
	case err != nil:
		fmt.Printf("outcome=client_error elapsed_ms=%d stalled_after_ms=%d sent=%d declared=%d err=%q\n",
			elapsed.Milliseconds(), stallAt.Milliseconds(), body.sent, *declare, err.Error())
	default:
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		fmt.Printf("outcome=responded status=%d elapsed_ms=%d stalled_after_ms=%d sent=%d declared=%d\n",
			resp.StatusCode, elapsed.Milliseconds(), stallAt.Milliseconds(), body.sent, *declare)
	}
}
