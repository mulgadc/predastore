package quicclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/quic-go/quic-go"
)

const (
	alpn = "mulga-repl-v1"
)

type Client struct {
	conn  *quic.Conn
	reqID uint64
}

func Dial(ctx context.Context, addr string) (*Client, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // demo only. Use mTLS with your CA in prod.
		NextProtos:         []string{alpn},
	}
	conn, err := quic.DialAddr(ctx, addr, tlsConf, &quic.Config{
		HandshakeIdleTimeout: 5 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
		MaxIdleTimeout:       60 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return nil
	//return c.conn.

	//WithError(0, "done")
}

func (c *Client) nextID() uint64 {
	return atomic.AddUint64(&c.reqID, 1)
}

/*
func (c *Client) Status(ctx context.Context) (map[string]any, error) {
	rh, meta, _, err := c.do(ctx, quicproto.MethodSTATUS, "", nil, nil, 0)
	if err != nil {
		return nil, err
	}
	if rh.Status != quicproto.StatusOK {
		return nil, fmt.Errorf("status: %d %s", rh.Status, string(meta))
	}
	var out map[string]any
	_ = json.Unmarshal(meta, &out)
	return out, nil
}

func (c *Client) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	rh, meta, _, err := c.do(ctx, quicproto.MethodPUT, key, nil, []byte("temp"), size)
	if err != nil {
		return err
	}
	if rh.Status != quicproto.StatusOK {
		return fmt.Errorf("put: %d %s", rh.Status, string(meta))
	}
	return nil
}
*/

func (c *Client) Get(ctx context.Context, objectRequest quicserver.ObjectRequest) (r io.Reader, err error) {

	objectRequestMarshalled, err := json.Marshal(objectRequest)
	if err != nil {
		fmt.Println(err)
	}

	rh, r, err := c.do(ctx, quicproto.MethodGET, objectRequestMarshalled, nil, 0)

	if err != nil {
		return nil, err
	}

	if rh.Status != quicproto.StatusOK {
		return nil, fmt.Errorf("get: %d %d", rh.Status, quicproto.StatusOK)
	}

	//_, err = io.Copy(w, body)

	fmt.Println("body")

	//body := make([]byte, 1024)
	//_, _ = r.Read(body)
	//spew.Dump(body)

	//_ = body.Close()

	return r, err
}

// do performs one RPC on one stream.
// request: header + key + meta + optional body
// response: header + meta + optional body (returned as ReadCloser)
func (c *Client) do(ctx context.Context, method uint8, objectRequest []byte, body []byte, bodyLen int64) (quicproto.Header, io.Reader, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return quicproto.Header{}, nil, err
	}

	// TODO: Optimise
	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)

	//br := bufio.NewReader(s)
	//bw := bufio.NewWriter(s)

	reqID := c.nextID()
	h := quicproto.Header{
		Version: quicproto.Version1,
		Method:  method,
		Status:  0,
		ReqID:   reqID,
		KeyLen:  uint32(len(objectRequest)),
		MetaLen: 0,
		BodyLen: 0,
	}

	// Write request
	if err := quicproto.WriteHeader(bw, h); err != nil {
		_ = s.Close()
		slog.Error("write header", "error", err)
		return quicproto.Header{}, nil, err
	}

	if _, err := bw.Write(objectRequest); err != nil {
		_ = s.Close()
		slog.Error("objectRequest", "error", err)
		return quicproto.Header{}, nil, err
	}

	// IMPORTANT: Flush the request bytes onto the QUIC stream.
	// Otherwise the server may never receive enough data to parse the header,
	// and the client will block forever waiting for the response header.
	if err := bw.Flush(); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, err
	}

	// Read response header + meta, then body stream (if any).
	respHdr, err := quicproto.ReadHeader(br)

	if err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, err
	}

	//var rc io.ReadCloser = nopReadCloser{r: br, closer: s}

	if respHdr.BodyLen == 0 {
		fmt.Println("No body expected")
		// No body expected. Close the stream.
		_ = s.Close()
		//rc = nil
	}
	return respHdr, br, nil
}

type nopReadCloser struct {
	r      io.Reader
	closer interface{ Close() error }
}

func (n nopReadCloser) Read(p []byte) (int, error) { return n.r.Read(p) }
func (n nopReadCloser) Close() error               { return n.closer.Close() }

func bytesReader(b []byte) io.Reader {
	return &byteReader{b: b}
}

type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
