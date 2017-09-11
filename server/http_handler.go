package server

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/akmistry/dory"
)

type Handler struct {
	c       *dory.Memcache
	bufPool *sync.Pool
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	key := []byte(req.URL.Path)
	if key[0] == '/' {
		key = key[1:]
	}
	if len(key) < h.c.MinKeySize() {
		resp.WriteHeader(http.StatusBadRequest)
		io.WriteString(resp, "Key too small")
		return
	} else if len(key) > h.c.MaxKeySize() {
		resp.WriteHeader(http.StatusBadRequest)
		io.WriteString(resp, "Key too large")
		return
	}

	switch req.Method {
	case "HEAD":
		// TODO: Return size.
		has := h.c.Has(key)
		if has {
			resp.WriteHeader(http.StatusOK)
		} else {
			resp.WriteHeader(http.StatusNotFound)
		}
	case "GET":
		buf := h.bufPool.Get().([]byte)[:0]
		defer h.bufPool.Put(buf)

		outBuf := h.c.Get(key, buf)
		if outBuf == nil {
			resp.WriteHeader(http.StatusNotFound)
			return
		}
		resp.Header().Add("Content-Length", fmt.Sprintf("%d", len(outBuf)))
		resp.Write(outBuf)
	case "PUT":
		if req.ContentLength < 0 {
			resp.WriteHeader(http.StatusLengthRequired)
			return
		} else if req.ContentLength < int64(h.c.MinValSize()) {
			resp.WriteHeader(http.StatusBadRequest)
			io.WriteString(resp, "Value too small")
			return
		} else if req.ContentLength > int64(h.c.MaxValSize()) {
			resp.WriteHeader(http.StatusBadRequest)
			io.WriteString(resp, "Value too large")
			return
		}

		buf := h.bufPool.Get().([]byte)
		defer h.bufPool.Put(buf)
		buf = buf[:cap(buf)]

		n, err := req.Body.Read(buf)
		if err != nil && err != io.EOF {
			resp.WriteHeader(http.StatusBadRequest)
			return
		} else if n != int(req.ContentLength) {
			resp.WriteHeader(http.StatusExpectationFailed)
			io.WriteString(resp, "Content read size != Content-Length header")
			return
		}
		h.c.Put(key, buf[:n])
	case "DELETE":
		h.c.Delete(key)
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func NewHandler(c *dory.Memcache) *Handler {
	pool := &sync.Pool{New: func() interface{} { return make([]byte, c.MaxValSize()) }}
	return &Handler{
		c:       c,
		bufPool: pool,
	}
}
