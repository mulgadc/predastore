// Command racedput writes one key from several clients at once and reports
// what each was told, so a caller can then read the key back and hold the
// cluster to the answer it gave.
//
// The AWS CLI cannot express this. Each invocation spends the better part of a
// second starting Python, and that jitter is orders of magnitude wider than the
// window being tested, so "concurrent" CLI writers arrive at the gate one after
// another and race nothing. Here the requests are built and signed up front and
// released together on a barrier, which is what puts them inside the gate at
// the same time.
//
//	go run ./scripts/bench/racedput -endpoints https://10.11.12.1:18443,... \
//	    -bucket b -key k -bodies g1.json,g2.json,g3.json \
//	    -access-key id -secret-key secret
//
// It prints one key=value line per writer naming the body it sent and the
// status it got, then a summary line. Exit 0 whenever every writer reached a
// verdict: which verdicts are correct is the caller's assertion, not this
// tool's. Exit 2 is being unable to run the probe at all.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const unsignedPayload = "UNSIGNED-PAYLOAD"

// writer is one client in the race: which body it sends, where it sends it,
// and what it was told.
type writer struct {
	name     string
	endpoint string
	body     []byte

	status int
	err    error
	took   time.Duration
}

func main() {
	endpoints := flag.String("endpoints", "", "comma-separated gate endpoints; writers are spread across them")
	bucket := flag.String("bucket", "", "bucket name")
	key := flag.String("key", "", "object key every writer writes")
	bodies := flag.String("bodies", "", "comma-separated files, one per writer")
	region := flag.String("region", "ap-southeast-2", "signing region")
	accessKey := flag.String("access-key", "", "access key id")
	secretKey := flag.String("secret-key", "", "secret access key")
	timeout := flag.Duration("timeout", 120*time.Second, "per-writer request timeout")
	flag.Parse()

	if *endpoints == "" || *bucket == "" || *key == "" || *bodies == "" || *accessKey == "" || *secretKey == "" {
		fmt.Fprintln(os.Stderr, "usage: racedput -endpoints <url,...> -bucket <b> -key <k> -bodies <f,...> -access-key <id> -secret-key <secret>")
		os.Exit(2)
	}

	hosts := strings.Split(*endpoints, ",")
	files := strings.Split(*bodies, ",")
	writers := make([]*writer, 0, len(files))
	for i, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read body %s: %v\n", f, err)
			os.Exit(2)
		}
		writers = append(writers, &writer{
			name:     strings.TrimSuffix(filepath.Base(f), filepath.Ext(f)),
			endpoint: hosts[i%len(hosts)],
			body:     content,
		})
	}

	// One transport per writer. A shared one would pool connections and could
	// serialise two writers onto one, which is the opposite of the point.
	newClient := func() *http.Client {
		return &http.Client{
			Timeout: *timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, //nolint:gosec // G402: local harness against a development CA.
					NextProtos:         []string{"http/1.1"},
				},
			},
		}
	}

	creds := aws.Credentials{AccessKeyID: *accessKey, SecretAccessKey: *secretKey}

	// Signed before the barrier, because signing is milliseconds of work and
	// doing it after the release would stagger exactly what is being aligned.
	reqs := make([]*http.Request, len(writers))
	for i, w := range writers {
		url := fmt.Sprintf("%s/%s/%s", w.endpoint, *bucket, *key)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, bytes.NewReader(w.body))
		if err != nil {
			fmt.Fprintf(os.Stderr, "build request: %v\n", err)
			os.Exit(2)
		}
		req.ContentLength = int64(len(w.body))
		req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
		if err := v4.NewSigner().SignHTTP(req.Context(), creds, req, unsignedPayload, "s3", *region, time.Now()); err != nil {
			fmt.Fprintf(os.Stderr, "sign request: %v\n", err)
			os.Exit(2)
		}
		reqs[i] = req
	}

	// Every goroutine is running and parked on the same closed-channel release,
	// so what separates the writers is scheduling alone.
	release := make(chan struct{})
	var ready, done sync.WaitGroup
	for i, w := range writers {
		ready.Add(1)
		done.Go(func() {
			client := newClient()
			ready.Done()
			<-release

			start := time.Now()
			resp, err := client.Do(reqs[i])
			w.took = time.Since(start)
			if err != nil {
				w.err = err

				return
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			w.status = resp.StatusCode
		})
	}
	ready.Wait()
	close(release)
	done.Wait()

	acked := 0
	for _, w := range writers {
		switch {
		case w.err != nil:
			fmt.Printf("writer=%s endpoint=%s outcome=client_error took_ms=%d err=%q\n",
				w.name, w.endpoint, w.took.Milliseconds(), w.err.Error())
		default:
			if w.status >= 200 && w.status < 300 {
				acked++
			}
			fmt.Printf("writer=%s endpoint=%s outcome=responded status=%d took_ms=%d\n",
				w.name, w.endpoint, w.status, w.took.Milliseconds())
		}
	}
	fmt.Printf("summary writers=%d acknowledged=%d\n", len(writers), acked)
}
