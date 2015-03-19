package msg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	zmq "github.com/pebbe/zmq4"
)

// Invalid struct
// Server rejects command as invalid
type Invalid struct {
	routingID []byte
	version   byte
}

// NewInvalid creates new Invalid message.
func NewInvalid() *Invalid {
	invalid := &Invalid{}
	return invalid
}

// String returns print friendly name.
func (i *Invalid) String() string {
	str := "ZGOSSIP_MSG_INVALID:\n"
	str += fmt.Sprintf("    version = %v\n", i.version)
	return str
}

// Marshal serializes the message.
func (i *Invalid) Marshal() ([]byte, error) {
	// Calculate size of serialized data
	bufferSize := 2 + 1 // Signature and message ID

	// version is a 1-byte integer
	bufferSize++

	// Now serialize the message
	tmpBuf := make([]byte, bufferSize)
	tmpBuf = tmpBuf[:0]
	buffer := bytes.NewBuffer(tmpBuf)
	binary.Write(buffer, binary.BigEndian, Signature)
	binary.Write(buffer, binary.BigEndian, InvalidID)

	// version
	value, _ := strconv.ParseUint("1", 10, 1*8)
	binary.Write(buffer, binary.BigEndian, byte(value))

	return buffer.Bytes(), nil
}

// Unmarshal unmarshals the message.
func (i *Invalid) Unmarshal(frames ...[]byte) error {
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
	if id != InvalidID {
		return errors.New("malformed Invalid message")
	}
	// version
	binary.Read(buffer, binary.BigEndian, &i.version)
	if i.version != 1 {
		return errors.New("malformed version message")
	}

	return nil
}

// Send sends marshaled data through 0mq socket.
func (i *Invalid) Send(socket *zmq.Socket) (err error) {
	frame, err := i.Marshal()
	if err != nil {
		return err
	}

	socType, err := socket.GetType()
	if err != nil {
		return err
	}

	// If we're sending to a ROUTER, we send the routingID first
	if socType == zmq.ROUTER {
		_, err = socket.SendBytes(i.routingID, zmq.SNDMORE)
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
func (i *Invalid) RoutingID() []byte {
	return i.routingID
}

// SetRoutingID sets the routingID for this message, routingID should be set
// whenever talking to a ROUTER.
func (i *Invalid) SetRoutingID(routingID []byte) {
	i.routingID = routingID
}

// SetVersion sets the version.
func (i *Invalid) SetVersion(version byte) {
	i.version = version
}

// Version returns the version.
func (i *Invalid) Version() byte {
	return i.version
}
