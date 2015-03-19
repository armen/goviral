package msg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	zmq "github.com/pebbe/zmq4"
)

// Pong struct
// Server responds to ping; note that pongs are not correlated with pings,
// and may be mixed with other commands, and the client should treat any
// incoming traffic as valid activity.
type Pong struct {
	routingID []byte
	version   byte
}

// NewPong creates new Pong message.
func NewPong() *Pong {
	pong := &Pong{}
	return pong
}

// String returns print friendly name.
func (p *Pong) String() string {
	str := "ZGOSSIP_MSG_PONG:\n"
	str += fmt.Sprintf("    version = %v\n", p.version)
	return str
}

// Marshal serializes the message.
func (p *Pong) Marshal() ([]byte, error) {
	// Calculate size of serialized data
	bufferSize := 2 + 1 // Signature and message ID

	// version is a 1-byte integer
	bufferSize++

	// Now serialize the message
	tmpBuf := make([]byte, bufferSize)
	tmpBuf = tmpBuf[:0]
	buffer := bytes.NewBuffer(tmpBuf)
	binary.Write(buffer, binary.BigEndian, Signature)
	binary.Write(buffer, binary.BigEndian, PongID)

	// version
	value, _ := strconv.ParseUint("1", 10, 1*8)
	binary.Write(buffer, binary.BigEndian, byte(value))

	return buffer.Bytes(), nil
}

// Unmarshal unmarshals the message.
func (p *Pong) Unmarshal(frames ...[]byte) error {
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
	if id != PongID {
		return errors.New("malformed Pong message")
	}
	// version
	binary.Read(buffer, binary.BigEndian, &p.version)
	if p.version != 1 {
		return errors.New("malformed version message")
	}

	return nil
}

// Send sends marshaled data through 0mq socket.
func (p *Pong) Send(socket *zmq.Socket) (err error) {
	frame, err := p.Marshal()
	if err != nil {
		return err
	}

	socType, err := socket.GetType()
	if err != nil {
		return err
	}

	// If we're sending to a ROUTER, we send the routingID first
	if socType == zmq.ROUTER {
		_, err = socket.SendBytes(p.routingID, zmq.SNDMORE)
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
func (p *Pong) RoutingID() []byte {
	return p.routingID
}

// SetRoutingID sets the routingID for this message, routingID should be set
// whenever talking to a ROUTER.
func (p *Pong) SetRoutingID(routingID []byte) {
	p.routingID = routingID
}

// SetVersion sets the version.
func (p *Pong) SetVersion(version byte) {
	p.version = version
}

// Version returns the version.
func (p *Pong) Version() byte {
	return p.version
}
