package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

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

	respOkResponse = []byte{'+', 'O', 'K', '\r', '\n'}

	respCmdSet = []byte{'S', 'E', 'T'}
	respCmdGet = []byte{'G', 'E', 'T'}
)

type respError struct {
	msg string
}

type respArray []interface{}

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
		buf := bufferpool.Get(allocLen)
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
		// Common case up to 4 elements, to avoid excessive allocations
		array := respArray(make([]interface{}, 0, 4))
		for i := 0; i < int(length); i++ {
			v, err := s.readMessage(r)
			if err != nil {
				return nil, err
			}
			array = append(array, v)
		}
		return array, nil

	default:
		return nil, fmt.Errorf("RedisServer: unexpected data type: 0x%02x", dataType)
	}

	return nil, nil
}

func (s *RedisServer) writeOkResponse(w *bufio.Writer) error {
	_, err := w.Write(respOkResponse)
	return err
}

func (s *RedisServer) writeBulk(w *bufio.Writer, val []byte) error {
	length := len(val)
	if val == nil {
		length = -1
	}

	lengthBuf := bufferpool.Get(16)
	defer bufferpool.Put(lengthBuf)

	*lengthBuf = (*lengthBuf)[:1]
	(*lengthBuf)[0] = '$'
	*lengthBuf = strconv.AppendInt(*lengthBuf, int64(length), 10)
	*lengthBuf = append(*lengthBuf, respCrlf...)
	_, err := w.Write(*lengthBuf)
	if err != nil {
		return err
	}
	if length < 0 {
		return nil
	}
	_, err = w.Write(val)
	if err != nil {
		return err
	}
	_, err = w.Write(respCrlf)
	return err
}

func (s *RedisServer) doCommand(cmd respArray, w *bufio.Writer) error {
	if len(cmd) < 1 {
		return fmt.Errorf("RedisServer: invalid command array length %d", len(cmd))
	}

	cmdBuf, ok := cmd[0].(*[]byte)
	if !ok {
		return fmt.Errorf("RedisServer: command not string")
	}
	if bytes.Equal(*cmdBuf, respCmdSet) {
		if len(cmd) < 3 {
			return fmt.Errorf("RedisServer: invalid SET array length %d", len(cmd))
		}
		key := cmd[1].(*[]byte)
		value := cmd[2].(*[]byte)
		s.c.Put(*key, *value)
		bufferpool.Put(key)
		bufferpool.Put(value)
		return s.writeOkResponse(w)
	} else if bytes.Equal(*cmdBuf, respCmdGet) {
		if len(cmd) < 2 {
			return fmt.Errorf("RedisServer: invalid GET array length %d", len(cmd))
		}
		key := cmd[1].(*[]byte)
		val := s.c.Get(*key, nil)
		bufferpool.Put(key)
		return s.writeBulk(w, val)
	}

	return fmt.Errorf("RedisServer: unsupported command %s", string(*cmdBuf))
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

		cmdArray, ok := cmd.(respArray)
		if !ok {
			return fmt.Errorf("RedisServer: request not array type")
		}
		err = s.doCommand(cmdArray, bufw)
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
