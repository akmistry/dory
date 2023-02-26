package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/akmistry/go-util/bufferpool"

	"github.com/akmistry/dory"
)

const (
	respTypeSimpleString = '+'
	respTypeError        = '-'
	respTypeInteger      = ':'
	respTypeBulkString   = '$'
	respTypeArray        = '*'

	// TODO: Revise these limits, or make them configurable.
	respStringMaxLength = 64 * 1024
	respBulkMaxLength   = 8 * 1024 * 1024
	respArrayMaxLength  = 64
)

var (
	respCrlf = []byte{'\r', '\n'}

	respResponseOk           = []byte{'+', 'O', 'K', '\r', '\n'}
	respResponseBulkArrayNil = []byte{'$', '-', '1', '\r', '\n'}

	respCmdSet    = []byte{'s', 'e', 't'}
	respCmdGet    = []byte{'g', 'e', 't'}
	respCmdDel    = []byte{'d', 'e', 'l'}
	respCmdExists = []byte{'e', 'x', 'i', 's', 't', 's'}

	respArrayPool = sync.Pool{New: func() interface{} {
		return &respArray{
			// Common case up to 4 elements, to avoid excessive allocations
			vals: make([]interface{}, 0, 4),
		}
	}}
)

type respError struct {
	msg string
}

type respArray struct {
	vals []interface{}
}

type RedisServer struct {
	c *dory.Memcache
}

func NewRedisServer(c *dory.Memcache) *RedisServer {
	return &RedisServer{
		c: c,
	}
}

func indexCrlf(buf []byte) int {
	// bytes.Index is a bit slow.
	//return bytes.Index(buf, respCrlf)
	bufLen := len(buf)
	i := bytes.IndexByte(buf, '\r')
	if i < 0 {
		return i
	}
	i++
	if i < bufLen && buf[i] == '\n' {
		return i - 1
	}

	for i < bufLen {
		crIndex := bytes.IndexByte(buf[i:], '\r')
		if crIndex < 0 {
			return crIndex
		}
		i += crIndex + 1
		if i < bufLen && buf[i] == '\n' {
			return i - 1
		}
	}
	return -1
}

func (s *RedisServer) readLine(r *bufio.Reader, out []byte) ([]byte, error) {
	for {
		bufLen := r.Buffered()
		// Expect at least 2 bytes for the CRLF
		if bufLen < 2 {
			_, err := r.Peek(2)
			if err != nil {
				return out, err
			}
			continue
		}

		buf, err := r.Peek(bufLen)
		if err != nil {
			// Don't expect this to ever happen
			panic(err)
		}

		end := indexCrlf(buf)
		discardBytes := 0
		readFinished := false
		if end == 0 {
			// Complete string already read. Done.
			r.Discard(2)
			break
		} else if end < 0 {
			end = len(buf)
			if buf[end-1] == '\r' {
				// Last byte could be the start of a CRLF, so consume up to the
				// second-last byte
				end--
			}
			discardBytes = end
		} else {
			// CRLF was found, so discard it
			discardBytes = end + 2
			readFinished = true
		}

		out = append(out, buf[:end]...)
		// Don't check the return value of Discard() because it is guaranteed to
		// succeed if 0 <= discardBytes <= r.Buffered(), which is always true here.
		r.Discard(discardBytes)
		if readFinished {
			break
		}
	}
	return out, nil
}

func (s *RedisServer) readInteger(r *bufio.Reader) (int64, error) {
	var val int64
	neg := false
	first := true
	for {
		bufLen := r.Buffered()
		// Expect at least 2 bytes for the CRLF
		if bufLen < 2 {
			_, err := r.Peek(2)
			if err != nil {
				return 0, err
			}
			continue
		}

		buf, _ := r.Peek(bufLen)

		for i, b := range buf {
			digit := b - '0'
			if digit > 9 {
				// Most common case, and quickest to check.
				if b == '\r' {
					// End of string. Assume there's a CRLF and discard bytes.
					// Don't check the return value of Discard() because any error
					// will be observed in the next Read.
					r.Discard(i + 2)
					if neg {
						val = -val
					}
					return val, nil
				}
				if b == '-' && i == 0 && first {
					neg = true
					continue
				}
				return 0, fmt.Errorf("readInteger: invalid character 0x%02x", b)
			}
			val *= 10
			val += int64(digit)
		}

		// Very lazy check that Peek() gave us the buffer we expected.
		// This check is here and not earlier because this code path is rare, and
		// the check is irrelevent for the common case.
		_ = buf[bufLen-1]

		r.Discard(bufLen)
		first = false
	}
}

func (s *RedisServer) readMessage(r *bufio.Reader) (interface{}, error) {
	dataType, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch dataType {
	case respTypeSimpleString:
		return s.readLine(r, nil)

	case respTypeError:
		msg, err := s.readLine(r, nil)
		if err != nil {
			return nil, err
		}
		return &respError{string(msg)}, nil

	case respTypeInteger:
		return s.readInteger(r)

	case respTypeBulkString:
		length, err := s.readInteger(r)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			// Null
			return nil, nil
		} else if length > respBulkMaxLength {
			return nil, fmt.Errorf("RedisServer: bulk string length %d > max %d",
				length, respBulkMaxLength)
		}
		allocLen := int(length + 2)
		if allocLen < (1 << bufferpool.MinSizeBits) {
			// Round up the buffer allocation to the minimum bufferpool size (16
			// bytes) to prevent extra allocations.
			allocLen = (1 << bufferpool.MinSizeBits)
		}
		buf := bufferpool.GetUninit(allocLen)
		*buf = (*buf)[:int(length+2)]
		_, err = io.ReadFull(r, *buf)
		if err != nil {
			return nil, err
		}
		// Just assume the last 2 bytes are CRLF and just drop them
		*buf = (*buf)[:int(length)]
		return buf, nil

	case respTypeArray:
		length, err := s.readInteger(r)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			// Null
			return nil, nil
		} else if length > respArrayMaxLength {
			return nil, fmt.Errorf("RedisServer: array length %d > max %d",
				length, respArrayMaxLength)
		}
		array := respArrayPool.Get().(*respArray)
		for i := 0; i < int(length); i++ {
			v, err := s.readMessage(r)
			if err != nil {
				return nil, err
			}
			array.vals = append(array.vals, v)
		}
		return array, nil

	default:
		return nil, fmt.Errorf("RedisServer: unexpected data type: 0x%02x", dataType)
	}

	return nil, nil
}

func (s *RedisServer) writeOkResponse(w *bufio.Writer) error {
	_, err := w.Write(respResponseOk)
	return err
}

func (s *RedisServer) writeBulk(w *bufio.Writer, val []byte) error {
	if val == nil {
		_, err := w.Write(respResponseBulkArrayNil)
		return err
	}

	length := len(val)

	var lengthBuf []byte
	if w.Available() > 22 {
		// We need a maximum of 22 bytes to write out an integer.
		// 1 byte for the type
		// 2 bytes for the CRLF
		// max 19 bytes for the positive int64
		lengthBuf = w.AvailableBuffer()[:1]
	} else {
		buf := bufferpool.GetUninit(22)
		defer bufferpool.Put(buf)
		lengthBuf = (*buf)[:1]
	}

	lengthBuf[0] = respTypeBulkString
	lengthBuf = strconv.AppendInt(lengthBuf, int64(length), 10)
	lengthBuf = append(lengthBuf, respCrlf...)
	_, err := w.Write(lengthBuf)
	if err != nil {
		return err
	}
	_, err = w.Write(val)
	if err != nil {
		return err
	}
	_, err = w.Write(respCrlf)
	return err
}

func (s *RedisServer) writeInteger(w *bufio.Writer, val int64) error {
	buf := bufferpool.GetUninit(16)
	defer bufferpool.Put(buf)

	*buf = (*buf)[:1]
	(*buf)[0] = respTypeInteger
	*buf = strconv.AppendInt(*buf, val, 10)
	*buf = append(*buf, respCrlf...)
	_, err := w.Write(*buf)
	return err
}

func equalsCommand(cmd, req []byte) bool {
	const lowerBit = 0x20
	if len(cmd) != len(req) {
		return false
	}
	for i, v := range cmd {
		if (v | lowerBit) != (req[i] | lowerBit) {
			return false
		}
	}
	return true
}

func (s *RedisServer) doCommand(cmd *respArray, w *bufio.Writer) error {
	if len(cmd.vals) < 1 {
		return fmt.Errorf("RedisServer: invalid command array length %d", len(cmd.vals))
	}

	cmdBuf, ok := cmd.vals[0].(*[]byte)
	if !ok {
		return fmt.Errorf("RedisServer: command not string")
	}
	// TODO: Hash-table command lookup, instead of this big if block.
	if equalsCommand(*cmdBuf, respCmdSet) {
		if len(cmd.vals) < 3 {
			return fmt.Errorf("RedisServer: invalid SET array length %d", len(cmd.vals))
		}
		key := cmd.vals[1].(*[]byte)
		value := cmd.vals[2].(*[]byte)
		s.c.Put(*key, *value)
		return s.writeOkResponse(w)
	} else if equalsCommand(*cmdBuf, respCmdGet) {
		if len(cmd.vals) < 2 {
			return fmt.Errorf("RedisServer: invalid GET array length %d", len(cmd.vals))
		}
		key := cmd.vals[1].(*[]byte)
		getBuf := bufferpool.GetUninit(s.c.MaxValSize())
		defer bufferpool.Put(getBuf)
		val := s.c.Get(*key, (*getBuf)[:0])
		return s.writeBulk(w, val)
	} else if equalsCommand(*cmdBuf, respCmdDel) {
		delCount := 0
		for i := 1; i < len(cmd.vals); i++ {
			key := cmd.vals[i].(*[]byte)
			s.c.Delete(*key)
			// TODO: Have Delete() return whether the key was actually removed, and
			// use that to incement delCount
			delCount++
		}
		return s.writeInteger(w, int64(delCount))
	} else if equalsCommand(*cmdBuf, respCmdExists) {
		existsCount := 0
		for i := 1; i < len(cmd.vals); i++ {
			key := cmd.vals[i].(*[]byte)
			if s.c.Has(*key) {
				existsCount++
			}
		}
		return s.writeInteger(w, int64(existsCount))
	}

	return fmt.Errorf("RedisServer: unsupported command %s", string(*cmdBuf))
}

func freeRespArray(a *respArray) {
	for i, v := range a.vals {
		switch v := v.(type) {
		case *[]byte:
			bufferpool.Put(v)
		}
		a.vals[i] = nil
	}
	a.vals = a.vals[:0]
	respArrayPool.Put(a)
}

func (s *RedisServer) Serve(conn io.ReadWriter) error {
	bufr := bufio.NewReader(conn)
	bufw := bufio.NewWriter(conn)
	for {
		cmd, err := s.readMessage(bufr)
		if err == io.EOF {
			// Connection closed. Non-error.
			break
		} else if err != nil {
			return err
		}

		cmdArray, ok := cmd.(*respArray)
		if !ok {
			return fmt.Errorf("RedisServer: request not array type")
		}
		err = s.doCommand(cmdArray, bufw)

		// Return the array to the pool
		freeRespArray(cmdArray)

		// Don't flush yet if there are commands still to be read
		if err == nil && bufr.Buffered() == 0 {
			err = bufw.Flush()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
