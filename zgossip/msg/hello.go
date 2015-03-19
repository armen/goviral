package msg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	zmq "github.com/pebbe/zmq4"
)

// Hello struct
// Client says hello to server
type Hello struct {
	routingID []byte
	version   byte
}

// NewHello creates new Hello message.
func NewHello() *Hello {
	hello := &Hello{}
	return hello
}

// String returns print friendly name.
func (h *Hello) String() string {
	str := "ZGOSSIP_MSG_HELLO:\n"
	str += fmt.Sprintf("    version = %v\n", h.version)
	return str
}

// Marshal serializes the message.
func (h *Hello) Marshal() ([]byte, error) {
	// Calculate size of serialized data
	bufferSize := 2 + 1 // Signature and message ID

	// version is a 1-byte integer
	bufferSize++

	// Now serialize the message
	tmpBuf := make([]byte, bufferSize)
	tmpBuf = tmpBuf[:0]
	buffer := bytes.NewBuffer(tmpBuf)
	binary.Write(buffer, binary.BigEndian, Signature)
	binary.Write(buffer, binary.BigEndian, HelloID)

	// version
	value, _ := strconv.ParseUint("1", 10, 1*8)
	binary.Write(buffer, binary.BigEndian, byte(value))

	return buffer.Bytes(), nil
}

// Unmarshal unmarshals the message.
func (h *Hello) Unmarshal(frames ...[]byte) error {
	if frames == nil {
		return errors.New("Can't unmarshal empty message")
	}

	frame := frames[0]
	frames = frames[1:]

	buffer := bytes.NewBuffer(frame)

	// Get and check protocol signature
	var signature uint16
	binary.Read(buffer, binary.BigEndian, &signature)
	if signature != Signature {
		return fmt.Errorf("invalid signature %X != %X", Signature, signature)
	}

	// Get message id and parse per message type
	var id uint8
	binary.Read(buffer, binary.BigEndian, &id)
	if id != HelloID {
		return errors.New("malformed Hello message")
	}
	// version
	binary.Read(buffer, binary.BigEndian, &h.version)
	if h.version != 1 {
		return errors.New("malformed version message")
	}

	return nil
}

// Send sends marshaled data through 0mq socket.
func (h *Hello) Send(socket *zmq.Socket) (err error) {
	frame, err := h.Marshal()
	if err != nil {
		return err
	}

	socType, err := socket.GetType()
	if err != nil {
		return err
	}

	// If we're sending to a ROUTER, we send the routingID first
	if socType == zmq.ROUTER {
		_, err = socket.SendBytes(h.routingID, zmq.SNDMORE)
		if err != nil {
			return err
		}
	}

	// Now send the data frame
	_, err = socket.SendBytes(frame, 0)
	if err != nil {
		return err
	}

	return err
}

// RoutingID returns the routingID for this message, routingID should be set
// whenever talking to a ROUTER.
func (h *Hello) RoutingID() []byte {
	return h.routingID
}

// SetRoutingID sets the routingID for this message, routingID should be set
// whenever talking to a ROUTER.
func (h *Hello) SetRoutingID(routingID []byte) {
	h.routingID = routingID
}

// SetVersion sets the version.
func (h *Hello) SetVersion(version byte) {
	h.version = version
}

// Version returns the version.
func (h *Hello) Version() byte {
	return h.version
}
