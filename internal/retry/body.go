package retry

import (
	"bytes"
	"io"
	"net/http"
)

const initialReadCapacity = 4096

type readCloser struct {
	io.Reader
	io.Closer
}

func bodyless(r *http.Request) bool {
	return r.Body == nil || r.Body == http.NoBody || (r.ContentLength == 0 && len(r.TransferEncoding) == 0)
}

// BufferBody reads the request body into memory, at most max bytes, so that it can be replayed.
// It reports whether the body can be replayed. When the body is not buffered, the request body
// is left byte-identical to the original stream.
func BufferBody(r *http.Request, max int64) (buf []byte, replayable bool) {
	if bodyless(r) {
		return nil, true
	}
	orig := r.Body
	if max <= 0 {
		return nil, false
	}
	if r.ContentLength > max {
		return nil, false
	}
	if r.ContentLength > 0 {
		buf = make([]byte, r.ContentLength)
		n, err := io.ReadFull(orig, buf)
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(buf))
			return buf, true
		}
		r.Body = readCloser{io.MultiReader(bytes.NewReader(buf[:n]), orig), orig}
		return nil, false
	}
	limit := max + 1
	data := make([]byte, 0, int(min(limit, initialReadCapacity)))
	lr := io.LimitReader(orig, limit)
	for {
		if len(data) == cap(data) {
			grown := make([]byte, len(data), int(min(int64(cap(data))*2, limit)))
			copy(grown, data)
			data = grown
		}
		n, err := lr.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err == io.EOF {
			break
		}
		if err != nil {
			r.Body = readCloser{io.MultiReader(bytes.NewReader(data), orig), orig}
			return nil, false
		}
	}
	if int64(len(data)) > max {
		r.Body = readCloser{io.MultiReader(bytes.NewReader(data), orig), orig}
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	return data, true
}
