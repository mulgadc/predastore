package transport

import "fmt"

// StreamError reports that a stream was aborted with an application error
// code, either locally via CancelRead/CancelWrite or by the peer. Callers
// unwrap it from Read/Write errors to recover the code.
type StreamError struct {
	Code StreamErrorCode
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("stream error: code %d", uint64(e.Code))
}
