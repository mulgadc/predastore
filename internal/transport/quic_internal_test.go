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
