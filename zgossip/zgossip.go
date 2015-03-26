// Package zgossip is a server implementation of the ZeroMQ Gossip Discovery Protocol
package zgossip

import (
	"container/list"

	msg "github.com/armen/goviral/zgossip/msg"
	zmq "github.com/pebbe/zmq4"
)

// myServer defines the context for each running server. Store
// whatever properties and structures you need for the server.
// myServer will be embedded in engine's server struct.
type myServer struct {
	remotes   *list.List        // Parents, as zmq.Socket instances
	tuples    *list.List        // Tuples list
	tuplesMap map[string]string // Tuples map for fast lookup
	curTuple  *tuple            // Holds current tuple to publish
	message   msg.Transit       // Message to broadcast
}

// myClient defines the state for each client connection. myClient
// will be embedded in engine's client struct. Note that all the actions
// are defined on client not this type but since this type is embedded
// in client they can access all the properties you defined here.
type myClient struct {
	curElem *list.Element // Holds current tuple to send
}

type tuple struct {
	key   string
	value string
}

// Allocate properties and structures for a new server instance.
func (s *server) init() error {
	s.remotes = list.New()
	s.tuples = list.New()
	s.tuplesMap = make(map[string]string)

	return nil
}

// Free properties and structures for a server instance
func (s *server) terminate() error {
	// Destroy properties here
	return nil
}

// Connect to a remote server
func (s *server) connect(endpoint string) error {
	remote, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		return err
	}

	// Never block on sending; we use an infinite HWM and buffer as many
	// messages as needed in outgoing pipes. Note that the maximum number
	// is the overall tuple set size.
	err = remote.SetSndhwm(0)
	if err != nil {
		return err
	}
	err = remote.SetRcvhwm(0)
	if err != nil {
		return err
	}

	// Send HELLO and then PUBLISH for each tuple we have
	h := msg.NewHello()
	h.Send(remote)

	for e := s.tuples.Front(); e != nil; e = e.Next() {
		t := e.Value.(*tuple)

		m := msg.NewPublish()
		m.Key = t.key
		m.Value = t.value
		m.Send(remote)
	}

	s.addSocketHandler(remote, zmq.POLLIN, func(e zmq.State) error { return s.remoteHandler(remote) })
	s.remotes.PushBack(remote)

	return nil
}

// Handle messages coming from remotes
func (s *server) remoteHandler(remote *zmq.Socket) error {
	t, err := msg.Recv(remote)
	if err != nil {
		return err
	}

	switch m := t.(type) {
	case *msg.Publish:
		s.accept(m.Key, m.Value)
	case *msg.Invalid:
		// Connection was reset, so send HELLO again
		h := msg.NewHello()
		h.Send(remote)
	case *msg.Pong:
		// Do nothing with PONGs
	}

	return nil
}

// Process an incoming tuple on this server.
func (s *server) accept(key, value string) {
	if v, ok := s.tuplesMap[value]; ok && v == value {
		// Duplicate tuple, do nothing
		return
	}

	// Create new tuple
	t := &tuple{key: key, value: value}
	s.tuplesMap[key] = value
	s.tuples.PushBack(t)

	// TODO(armen): Deliver to calling application

	// Hold in server context so we can broadcast to all clients
	// Hold the tuple in server.curTuple so it's available to all clients
	s.curTuple = t
	s.broadcastEvent(forwardEvent)

	// Copy new tuple announcement to all remotes
	for r := s.remotes.Front(); r != nil; r = r.Next() {
		remote := r.Value.(*zmq.Socket)

		m := msg.NewPublish()
		m.Key = t.key
		m.Value = t.value
		m.Send(remote)
	}
}

// Process server API method, return reply message if any
func (s *server) method(msg *cmd) (*resp, error) {
	return nil, nil
}

// Allocate properties and structures for a new client connection and
// optionally c.setNextEvent(event).
func (c *client) init() error {
	return nil
}

// Free properties and structures for a client connection
func (c *client) terminate() error {
	// Destroy properties here
	return nil
}

// getFirstTuple
func (c *client) getFirstTuple() error {
	c.prepareTuple(c.server.tuples.Front())
	return nil
}

// getNextTuple
func (c *client) getNextTuple() error {
	if c.curElem == nil {
		c.setNextEvent(finishedEvent)
	}

	c.prepareTuple(c.curElem.Next())

	return nil
}

func (c *client) prepareTuple(elem *list.Element) {
	c.curElem = elem
	if c.curElem != nil {
		t := c.curElem.Value.(*tuple)
		m := msg.NewPublish()
		m.Key = t.key
		m.Value = t.value
		c.server.message = m
		c.setNextEvent(okEvent)
	} else {
		c.setNextEvent(finishedEvent)
	}
}

// storeTupleIfNew
func (c *client) storeTupleIfNew() error {
	m := c.server.message.(*msg.Publish)
	c.server.accept(m.Key, m.Value)

	return nil
}

// getTupleToForward
func (c *client) getTupleToForward() error {
	// The whole broadcast operation happens in one thread
	// so there's no risk of confusion here.
	m := msg.NewPublish()
	m.Key = c.server.curTuple.key
	m.Value = c.server.curTuple.value
	c.server.message = m

	return nil
}
