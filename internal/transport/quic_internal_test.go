package transport

import (
	"errors"
	"testing"

	"github.com/quic-go/quic-go"
)

func TestTranslateConnErr(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		closed bool
	}{
		{"remote plain close", &quic.ApplicationError{ErrorCode: 0, Remote: true}, true},
		{"local plain close", &quic.ApplicationError{ErrorCode: 0}, true},
		{"remote listener shutdown", &quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(ConnCodeShutdown), Remote: true}, true},
		{"unknown application code", &quic.ApplicationError{ErrorCode: 7, Remote: true}, false},
		{"idle timeout", &quic.IdleTimeoutError{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := translateConnErr(tc.err)
			if errors.Is(got, ErrConnClosed) != tc.closed {
				t.Fatalf("translateConnErr(%v) = %v, want ErrConnClosed: %v", tc.err, got, tc.closed)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("translateConnErr(%v) = %v, lost the quic error", tc.err, got)
			}
		})
	}
}

// A stream operation fails with a connection-level error when the peer closes
// the whole connection mid-stream, so the stream path must classify those the
// same way AcceptStream does while still mapping stream aborts to StreamError.
func TestTranslateQUICErrClassifiesConnectionClosesAndStreamAborts(t *testing.T) {
	var streamErr *StreamError
	got := translateQUICErr(&quic.StreamError{ErrorCode: 4})
	if !errors.As(got, &streamErr) {
		t.Fatalf("translateQUICErr(stream abort) = %v, want *StreamError", got)
	}
	if streamErr.Code != StreamErrorCode(4) {
		t.Fatalf("StreamError.Code = %v, want 4", streamErr.Code)
	}

	closes := []error{
		&quic.ApplicationError{ErrorCode: 0, Remote: true},
		&quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(ConnCodeShutdown), Remote: true},
	}
	for _, err := range closes {
		if got := translateQUICErr(err); !errors.Is(got, ErrConnClosed) {
			t.Fatalf("translateQUICErr(%v) = %v, want ErrConnClosed", err, got)
		}
	}

	fault := &quic.ApplicationError{ErrorCode: 7, Remote: true}
	if got := translateQUICErr(fault); errors.Is(got, ErrConnClosed) {
		t.Fatalf("translateQUICErr(%v) = %v, must not read an unknown code as a close", fault, got)
	}
	if translateQUICErr(nil) != nil {
		t.Fatal("translateQUICErr(nil) must stay nil")
	}
}
