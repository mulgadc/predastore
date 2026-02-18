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
	"github.com/mulgadc/predastore/utils"
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
	if c.conn != nil {
		return c.conn.CloseWithError(0, "done")
	}
	return nil
}

func (c *Client) nextID() uint64 {
	return atomic.AddUint64(&c.reqID, 1)
}

// Put sends a shard to the QUIC server and returns the WriteResult
func (c *Client) Put(ctx context.Context, putReq quicserver.PutRequest, shardData io.Reader) (*quicserver.PutResponse, error) {
	slog.Debug("QUIC Put starting",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
		"shardSize", putReq.ShardSize,
	)

	putReqBytes, err := json.Marshal(putReq)
	if err != nil {
		return nil, fmt.Errorf("marshal put request: %w", err)
	}

	rh, respMeta, err := c.doPut(ctx, putReqBytes, shardData, int64(putReq.ShardSize))
	if err != nil {
		slog.Error("QUIC Put doPut failed",
			"bucket", putReq.Bucket,
			"shardIndex", putReq.ShardIndex,
			"error", err,
		)
		return nil, fmt.Errorf("put request failed: %w", err)
	}

	slog.Debug("QUIC Put doPut completed",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
		"status", rh.Status,
	)

	if rh.Status != quicproto.StatusOK {
		return nil, fmt.Errorf("put: status %d", rh.Status)
	}

	var response quicserver.PutResponse
	if err := json.Unmarshal(respMeta, &response); err != nil {
		return nil, fmt.Errorf("unmarshal put response: %w", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("put error: %s", response.Error)
	}

	slog.Debug("QUIC Put completed successfully",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
		"shardNum", response.WriteResult.ShardNum,
	)

	return &response, nil
}

// doPut performs a PUT RPC with body streaming
func (c *Client) doPut(ctx context.Context, requestBytes []byte, body io.Reader, bodyLen int64) (quicproto.Header, []byte, error) {
	slog.Debug("doPut: opening stream")
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		slog.Error("doPut: failed to open stream", "error", err)
		return quicproto.Header{}, nil, err
	}
	slog.Debug("doPut: stream opened", "streamID", s.StreamID())

	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)

	reqID := c.nextID()
	h := quicproto.Header{
		Version: quicproto.Version1,
		Method:  quicproto.MethodPUT,
		Status:  0,
		ReqID:   reqID,
		KeyLen:  utils.IntToUint32(len(requestBytes)),
		MetaLen: 0,
		BodyLen: utils.Int64ToUint64(bodyLen),
	}

	// Write request header
	if err := quicproto.WriteHeader(bw, h); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("write header: %w", err)
	}

	// Write request metadata (PutRequest JSON)
	if _, err := bw.Write(requestBytes); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("write request: %w", err)
	}

	// Flush header and request before streaming body
	if err := bw.Flush(); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("flush header: %w", err)
	}

	// Stream the body data
	slog.Debug("doPut: streaming body", "bodyLen", bodyLen)
	written, err := io.CopyN(bw, body, bodyLen)
	if err != nil {
		slog.Error("doPut: body write failed", "written", written, "bodyLen", bodyLen, "error", err)
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("write body: %w (wrote %d of %d)", err, written, bodyLen)
	}
	slog.Debug("doPut: body written", "written", written)

	// Flush the body
	if err := bw.Flush(); err != nil {
		slog.Error("doPut: flush body failed", "error", err)
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("flush body: %w", err)
	}
	slog.Debug("doPut: body flushed, waiting for response")

	// Read response header
	respHdr, err := quicproto.ReadHeader(br)
	if err != nil {
		slog.Error("doPut: read response header failed", "error", err)
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("read response header: %w", err)
	}
	slog.Debug("doPut: response header received", "status", respHdr.Status, "metaLen", respHdr.MetaLen)

	// Read response metadata
	var respMeta []byte
	if respHdr.MetaLen > 0 {
		respMeta = make([]byte, respHdr.MetaLen)
		if _, err := io.ReadFull(br, respMeta); err != nil {
			_ = s.Close()
			return quicproto.Header{}, nil, fmt.Errorf("read response meta: %w", err)
		}
	}

	// Close both sides of the stream to fully release it:
	// - CancelRead(0): close read side (we've read all expected data)
	// - Close(): close write side (sends FIN)
	// Both are needed for the stream to be fully released in QUIC.
	s.CancelRead(0)
	_ = s.Close()
	slog.Debug("doPut: stream closed", "streamID", s.StreamID())
	return respHdr, respMeta, nil
}

// Delete sends a delete request to the QUIC server for a shard
func (c *Client) Delete(ctx context.Context, delReq quicserver.DeleteRequest) (*quicserver.DeleteResponse, error) {
	delReqBytes, err := json.Marshal(delReq)
	if err != nil {
		return nil, fmt.Errorf("marshal delete request: %w", err)
	}

	rh, respMeta, err := c.doDelete(ctx, delReqBytes)
	if err != nil {
		return nil, fmt.Errorf("delete request failed: %w", err)
	}

	if rh.Status != quicproto.StatusOK {
		return nil, fmt.Errorf("delete: status %d", rh.Status)
	}

	var response quicserver.DeleteResponse
	if err := json.Unmarshal(respMeta, &response); err != nil {
		return nil, fmt.Errorf("unmarshal delete response: %w", err)
	}

	if response.Error != "" {
		return nil, fmt.Errorf("delete error: %s", response.Error)
	}

	return &response, nil
}

// doDelete performs a DELETE RPC (no body)
func (c *Client) doDelete(ctx context.Context, requestBytes []byte) (quicproto.Header, []byte, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return quicproto.Header{}, nil, err
	}

	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)

	reqID := c.nextID()
	h := quicproto.Header{
		Version: quicproto.Version1,
		Method:  quicproto.MethodDELETE,
		Status:  0,
		ReqID:   reqID,
		KeyLen:  utils.IntToUint32(len(requestBytes)),
		MetaLen: 0,
		BodyLen: 0,
	}

	// Write request header
	if err := quicproto.WriteHeader(bw, h); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("write header: %w", err)
	}

	// Write request metadata (DeleteRequest JSON)
	if _, err := bw.Write(requestBytes); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("write request: %w", err)
	}

	// Flush
	if err := bw.Flush(); err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("flush: %w", err)
	}

	// Read response header
	respHdr, err := quicproto.ReadHeader(br)
	if err != nil {
		_ = s.Close()
		return quicproto.Header{}, nil, fmt.Errorf("read response header: %w", err)
	}

	// Read response metadata
	var respMeta []byte
	if respHdr.MetaLen > 0 {
		respMeta = make([]byte, respHdr.MetaLen)
		if _, err := io.ReadFull(br, respMeta); err != nil {
			_ = s.Close()
			return quicproto.Header{}, nil, fmt.Errorf("read response meta: %w", err)
		}
	}

	// Close both sides of the stream to fully release it
	s.CancelRead(0)
	_ = s.Close()
	return respHdr, respMeta, nil
}

// Get retrieves a full shard from the QUIC server.
// IMPORTANT: The returned io.ReadCloser MUST be closed by the caller to release the QUIC stream.
func (c *Client) Get(ctx context.Context, objectRequest quicserver.ObjectRequest) (io.ReadCloser, error) {
	objectRequestMarshalled, err := json.Marshal(objectRequest)
	if err != nil {
		return nil, fmt.Errorf("marshal object request: %w", err)
	}

	rh, rc, err := c.do(ctx, quicproto.MethodGET, objectRequestMarshalled, nil, 0)
	if err != nil {
		return nil, err
	}

	if rh.Status != quicproto.StatusOK {
		if rc != nil {
			rc.Close()
		}
		return nil, fmt.Errorf("get: status %d (expected %d)", rh.Status, quicproto.StatusOK)
	}

	return rc, nil
}

// limitedReadCloser wraps a limited reader and the underlying closer.
type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func (l *limitedReadCloser) Close() error {
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

// GetRange retrieves a byte range from a shard on the QUIC server.
// This is an optimized path for partial reads (e.g., viperblock pread operations).
// IMPORTANT: The returned io.ReadCloser MUST be closed by the caller to release the QUIC stream.
func (c *Client) GetRange(ctx context.Context, objectRequest quicserver.ObjectRequest) (io.ReadCloser, error) {
	// ObjectRequest now includes RangeStart and RangeEnd fields
	objectRequestMarshalled, err := json.Marshal(objectRequest)
	if err != nil {
		return nil, fmt.Errorf("marshal object request: %w", err)
	}

	rh, rc, err := c.do(ctx, quicproto.MethodGET, objectRequestMarshalled, nil, 0)
	if err != nil {
		return nil, err
	}

	if rh.Status != quicproto.StatusOK {
		if rc != nil {
			rc.Close()
		}
		return nil, fmt.Errorf("get range: status %d (expected %d)", rh.Status, quicproto.StatusOK)
	}

	// Return a limited reader that also closes the underlying stream when done
	if rh.BodyLen > 0 {
		return &limitedReadCloser{
			Reader: io.LimitReader(rc, utils.Uint64ToInt64(rh.BodyLen)),
			closer: rc,
		}, nil
	}

	return rc, nil
}

// streamReadCloser wraps a reader and a QUIC stream, closing the stream when done.
// This is CRITICAL for connection pooling - unclosed streams will accumulate
// and eventually block OpenStreamSync() when the stream limit is reached.
type streamReadCloser struct {
	r      io.Reader
	stream *quic.Stream
}

func (s *streamReadCloser) Read(p []byte) (int, error) {
	return s.r.Read(p)
}

func (s *streamReadCloser) Close() error {
	if s.stream != nil {
		// Close both sides of the stream to fully release it:
		// - CancelRead(0): close read side (caller is done reading)
		// - Close(): close write side (sends FIN)
		s.stream.CancelRead(0)
		return s.stream.Close()
	}
	return nil
}

// do performs one RPC on one stream.
// request: header + key + meta + optional body
// response: header + meta + optional body (returned as ReadCloser)
//
// IMPORTANT: The returned io.ReadCloser MUST be closed by the caller to release
// the QUIC stream. Failure to close will cause stream exhaustion with pooled connections.
func (c *Client) do(ctx context.Context, method uint8, objectRequest []byte, body []byte, bodyLen int64) (quicproto.Header, io.ReadCloser, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return quicproto.Header{}, nil, err
	}

	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)

	reqID := c.nextID()
	h := quicproto.Header{
		Version: quicproto.Version1,
		Method:  method,
		Status:  0,
		ReqID:   reqID,
		KeyLen:  utils.IntToUint32(len(objectRequest)),
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

	if respHdr.BodyLen == 0 {
		// No body expected. Close both sides of the stream now.
		s.CancelRead(0)
		_ = s.Close()
		return respHdr, nil, nil
	}

	// Return a ReadCloser that will close the stream when the caller is done reading
	return respHdr, &streamReadCloser{r: br, stream: s}, nil
}
