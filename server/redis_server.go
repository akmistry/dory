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
		}

		out = append(out, buf[:end]...)
		_, err = r.Discard(end)
		if err != nil {
			// Don't expect this to ever happen
			panic(err)
		}
	}
	return out, nil
}

func fastParseInt(buf []byte) (int64, error) {
	neg := false
	val := int64(0)
	for i, b := range buf {
		if i == 0 && b == '-' {
			neg = true
			continue
		}
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("fastParseInt: invalid character 0x%02x", b)
		}
		val *= 10
		val += int64(b - '0')
	}
	if neg {
		val = -val
	}
	return val, nil
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
		str, err := s.readLine(r, nil)
		if err != nil {
			return nil, err
		}
		return fastParseInt(str)

	case respTypeBulkString:
		lenStr := make([]byte, 0, 16)
		lenStr, err := s.readLine(r, lenStr)
		if err != nil {
			return nil, err
		}
		length, err := fastParseInt(lenStr)
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
		allocLen := int(length)
		if allocLen < (1 << bufferpool.MinSizeBits) {
			// Round up the buffer allocation to the minimum bufferpool size (16
			// bytes) to prevent extra allocations.
			allocLen = (1 << bufferpool.MinSizeBits)
		}
		buf := bufferpool.GetUninit(allocLen)
		*buf = (*buf)[:int(length)]
		_, err = io.ReadFull(r, *buf)
		if err != nil {
			return nil, err
		}
		// Check for CRLF and discard
		peekBuf, err := r.Peek(2)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(peekBuf, respCrlf) {
			return nil, fmt.Errorf("RedisServer: bulk string does not end in CRLF")
		}
		r.Discard(2)
		return buf, nil

	case respTypeArray:
		lenStr := make([]byte, 0, 16)
		lenStr, err := s.readLine(r, lenStr)
		if err != nil {
			return nil, err
		}
		length, err := fastParseInt(lenStr)
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
