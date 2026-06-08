package httpserver

import "net/http"

// capturingWriter is an http.ResponseWriter wrapper that records whether
// WriteHeader (or Write) was called, the status code, and the number of bytes
// written. It is shared between AccessLog (which needs status + bytes for the
// access-log line) and Recover (which needs wroteHeader to decide whether it
// is safe to write a 500 problem response after a panic).
//
// Design note: each middleware that needs capture (AccessLog, Recover) wraps
// its own capturingWriter. This is slightly redundant when both are in the
// same chain, but it avoids tight coupling and is always correct. The outer
// AccessLog wrapper sees the final status because the inner Recover wrapper
// delegates to it; both correctly track their own view of the response.
type capturingWriter struct {
	http.ResponseWriter
	wroteHeader bool
	status      int
	bytes       int
}

// WriteHeader records the status code and marks the header as committed.
func (cw *capturingWriter) WriteHeader(code int) {
	if !cw.wroteHeader {
		cw.wroteHeader = true
		cw.status = code
	}
	cw.ResponseWriter.WriteHeader(code)
}

// Write marks the header committed (with an implicit 200) on first call and
// accumulates the byte count.
func (cw *capturingWriter) Write(b []byte) (int, error) {
	if !cw.wroteHeader {
		cw.wroteHeader = true
		cw.status = http.StatusOK
	}
	n, err := cw.ResponseWriter.Write(b)
	cw.bytes += n
	return n, err
}

// Status returns the status code written, or 200 if nothing was written yet
// (matches the implicit default of http.ResponseWriter).
func (cw *capturingWriter) Status() int {
	if cw.status == 0 {
		return http.StatusOK
	}
	return cw.status
}

// BytesWritten returns the total number of bytes written to the body.
func (cw *capturingWriter) BytesWritten() int { return cw.bytes }

// WroteHeader reports whether WriteHeader or Write was called.
func (cw *capturingWriter) WroteHeader() bool { return cw.wroteHeader }
