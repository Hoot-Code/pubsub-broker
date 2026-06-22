// Package wsutil provides a minimal, stdlib-only RFC 6455 WebSocket server
// implementation shared by both the optional HTTP/WebSocket gateway and the
// broker's embedded admin server (Explorer endpoint). The implementation
// supports unfragmented text frames (opcode 0x1), ping/pong (0x9/0xA), and
// close (0x8). Binary, fragmented, and compressed frames are not supported.
//
// No external dependency is used, per project policy.
package wsutil

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
)

// websocketGUID is the fixed RFC 6455 handshake GUID.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcodes used by the minimal frame implementation.
const (
	OpText  = 0x1
	OpClose = 0x8
	OpPing  = 0x9
	OpPong  = 0xA
)

// Conn is a minimal RFC 6455 WebSocket connection supporting text frames,
// ping/pong, and close. Binary, fragmented, and compressed frames are not
// supported — sufficient for the gateway's and Explorer's JSON-over-text-frame
// protocol.
type Conn struct {
	Conn net.Conn
	Buf  *bufio.Reader
}

// ComputeAcceptKey computes the Sec-WebSocket-Accept header value for the
// given Sec-WebSocket-Key per RFC 6455 §1.3:
// base64(sha1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")).
func ComputeAcceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(websocketGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// UpgradeWebSocket performs the RFC 6455 handshake on r/w and, on success,
// hijacks the underlying TCP connection and returns a Conn ready for
// ReadTextFrame/WriteTextFrame. The caller owns the returned connection and
// must call Close when done.
func UpgradeWebSocket(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if r.Method != http.MethodGet {
		return nil, fmt.Errorf("websocket: method %s not allowed", r.Method)
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("websocket: missing Sec-WebSocket-Key")
	}
	if upgrade := r.Header.Get("Upgrade"); upgrade == "" {
		return nil, errors.New("websocket: missing Upgrade header")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("websocket: ResponseWriter does not support hijacking")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, fmt.Errorf("websocket: hijack: %w", err)
	}

	accept := ComputeAcceptKey(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := rw.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("websocket: write handshake: %w", err)
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("websocket: flush handshake: %w", err)
	}

	return &Conn{Conn: conn, Buf: rw.Reader}, nil
}

// ReadTextFrame blocks until a complete text frame (opcode 0x1) is read and
// returns its payload as a string. Ping frames are answered with a pong and
// skipped transparently; a close frame returns an error.
func (c *Conn) ReadTextFrame() (string, error) {
	for {
		opcode, payload, err := c.readFrame()
		if err != nil {
			return "", err
		}
		switch opcode {
		case OpText:
			return string(payload), nil
		case OpPing:
			if err := c.writeFrame(OpPong, payload); err != nil {
				return "", err
			}
		case OpPong:
			// Ignore unsolicited pongs.
		case OpClose:
			_ = c.writeFrame(OpClose, nil)
			return "", errors.New("websocket: connection closed by peer")
		default:
			return "", fmt.Errorf("websocket: unsupported opcode %#x", opcode)
		}
	}
}

// WriteTextFrame sends data as a single unmasked text frame (servers do not
// mask frames per RFC 6455 §5.1).
func (c *Conn) WriteTextFrame(data string) error {
	return c.writeFrame(OpText, []byte(data))
}

// Close sends a close frame (best-effort) and closes the underlying
// connection.
func (c *Conn) Close() error {
	_ = c.writeFrame(OpClose, nil)
	return c.Conn.Close()
}

// readFrame reads one WebSocket frame from the connection and returns its
// opcode and unmasked payload. Client-to-server frames are always masked
// per RFC 6455 §5.1; this implementation unmasks them and rejects frames
// that claim to be unmasked, as required of a compliant server.
func (c *Conn) readFrame() (opcode byte, payload []byte, err error) {
	hdr := make([]byte, 2)
	if _, err = ReadFull(c.Buf, hdr); err != nil {
		return 0, nil, err
	}
	fin := hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	length := int64(hdr[1] & 0x7F)

	if !fin {
		return 0, nil, errors.New("websocket: fragmented frames not supported")
	}
	if !masked {
		return 0, nil, errors.New("websocket: client frame must be masked")
	}

	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err = ReadFull(c.Buf, ext); err != nil {
			return 0, nil, err
		}
		length = int64(ext[0])<<8 | int64(ext[1])
	case 127:
		ext := make([]byte, 8)
		if _, err = ReadFull(c.Buf, ext); err != nil {
			return 0, nil, err
		}
		length = 0
		for _, b := range ext {
			length = length<<8 | int64(b)
		}
	}

	maskKey := make([]byte, 4)
	if _, err = ReadFull(c.Buf, maskKey); err != nil {
		return 0, nil, err
	}

	payload = make([]byte, length)
	if _, err = ReadFull(c.Buf, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= maskKey[i%4]
	}
	return opcode, payload, nil
}

// writeFrame writes one unmasked WebSocket frame with the given opcode and
// payload to the connection.
func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	var hdr []byte
	first := byte(0x80) | opcode // FIN=1
	n := len(payload)
	switch {
	case n <= 125:
		hdr = []byte{first, byte(n)}
	case n <= 0xFFFF:
		hdr = []byte{first, 126, byte(n >> 8), byte(n)}
	default:
		hdr = make([]byte, 10)
		hdr[0] = first
		hdr[1] = 127
		for i := 0; i < 8; i++ {
			hdr[2+i] = byte(n >> (8 * (7 - i)))
		}
	}
	if _, err := c.Conn.Write(hdr); err != nil {
		return err
	}
	if n > 0 {
		if _, err := c.Conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadFull reads exactly len(buf) bytes from r, like io.ReadFull, without
// importing io solely for this one call site elsewhere in the file.
func ReadFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
