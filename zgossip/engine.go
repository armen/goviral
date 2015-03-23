// Package zgossip is 100% generated. If you edit this file,
// you will lose your changes at the next build cycle.
// DO NOT MAKE ANY CHANGES YOU WISH TO KEEP.
//
// The correct places for commits are:
//  - The XML model used for this code generation: zgossip.xml
//  - The code generation script that built this file: zproto_server_go
package zgossip

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	msg "github.com/armen/goviral/zgossip/msg"
	"github.com/jtacoma/go-zpl"
	zmq "github.com/pebbe/zmq4"
)

// State machine constants
type state int

const (
	startState state = iota + 1
	haveTupleState
	connectedState
	externalState
)

// Events
type event int

const (
	nullEvent event = iota
	terminateEvent
	helloEvent
	okEvent
	finishedEvent
	publishEvent
	forwardEvent
	pingEvent
	expiredEvent
)

const (
	pipeInterval = time.Second * 1
	confInterval = time.Second * 1
	defTimeout   = time.Second * 5

	// Port range 0xc000~0xffff is defined by IANA for dynamic or private ports
	// We use this when choosing a port for dynamic binding
	dynPortFrom uint16 = 0xc000
	dynPortTo   uint16 = 0xffff
)

var eventName = map[event]string{
	nullEvent:      "(NONE)",
	terminateEvent: "terminate",
	helloEvent:     "HELLO",
	okEvent:        "ok",
	finishedEvent:  "finished",
	publishEvent:   "PUBLISH",
	forwardEvent:   "forward",
	pingEvent:      "PING",
	expiredEvent:   "expired",
}

type cmd struct {
	method string
	arg    interface{}
}

const (
	cmdSave    = "SAVE"
	cmdSet     = "SET"
	cmdVerbose = "VERBOSE"
	cmdPort    = "PORT"
	cmdLoad    = "LOAD"
	cmdBind    = "BIND"
	cmdTerm    = "$TERM"
)

type resp struct {
	method  string
	payload interface{}
	err     error
}

// Context for the whole server task. This embeds the application-level
// server context so we can access to methods and properties defined in
// application-level server.
type server struct {
	myServer // Application-level server context

	pipe       chan interface{}       // Channel to back to caller API
	router     *zmq.Socket            // Socket to talk to clients
	port       uint16                 // Server port bound to
	loop       *zmq.Reactor           // Reactor for server sockets
	message    msg.Transit            // Message in and out
	clients    map[string]*client     // Clients we're connected to
	config     map[string]interface{} // Configuration tree
	configInfo os.FileInfo            // Configuration file info
	clientID   uint                   // Client identifier counter
	timeout    time.Duration          // Default client expiry timeout
	verbose    bool                   // Verbose logging enabled?
	logPrefix  string                 // Default log prefix
}

// Context for each connected client. This embeds the application-level
// client context so we can access to methods and properties defined in
// application-level client.
type client struct {
	myClient // Application-level client context

	server    *server // Reference to parent server
	hashKey   string  // Key into server.clients map
	routingID []byte  // Routing_id back to client
	uniqueID  uint    // Client identifier in server
	state     state   // Current state
	event     event   // Current event
	nextEvent event   // Next event
	exception event   // Exception event, if any
	ticket    uint64  // Reactor ticket for client timeouts
	logPrefix string  // Log prefix string

	// TODO(armen): Implement wakeup timer
	// wakeupEvent event   // Wake up with this event
}

func newServer() (*server, error) {
	s := &server{
		pipe:    make(chan interface{}),
		loop:    zmq.NewReactor(),
		clients: make(map[string]*client),
		config:  make(map[string]interface{}),
		timeout: defTimeout,
	}
	var err error
	s.router, err = zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	// By default the socket will discard outgoing messages above the
	// HWM of 1,000. This isn't helpful for high-volume streaming. We
	// will use a unbounded queue here. If applications need to guard
	// against queue overflow, they should use a credit-based flow
	// control scheme.
	err = s.router.SetSndhwm(0)
	if err != nil {
		return nil, err
	}
	err = s.router.SetRcvhwm(0)
	if err != nil {
		return nil, err
	}

	s.loop.AddSocket(s.router, zmq.POLLIN, func(e zmq.State) error { return s.handleProtocol() })
	s.loop.AddChannel(s.pipe, 1, func(msg interface{}) error { return s.handlePipe(msg) })
	s.loop.AddChannelTime(time.Tick(confInterval), 1, func(i interface{}) error { return s.watchConfig() })

	rand.Seed(time.Now().UTC().UnixNano())
	s.clientID = uint(rand.Intn(1000))

	return s, nil
}

func (s *server) newClient() *client {

	s.clientID++

	c := &client{
		server:   s,
		hashKey:  fmt.Sprintf("%x", s.message.RoutingID()),
		uniqueID: s.clientID,
	}
	copy(c.routingID, s.message.RoutingID())

	// If expiry timers are being used, create client timeout handler
	if s.timeout != 0 {
		c.ticket = s.loop.AddChannelTime(time.Tick(s.timeout), 1, func(i interface{}) error { return c.handleTimeout() })
	}

	// Give application chance to initialize and set next event
	c.state = startState
	c.event = nullEvent
	c.init()

	return c
}

// Process message from pipe
func (s *server) handlePipe(i interface{}) (err error) {

	msg := i.(*cmd)

	switch msg.method {
	case cmdVerbose:
		s.verbose = true

	case cmdTerm:
		return errors.New("Terminating")

	case cmdBind:
		endpoint := msg.arg.(string)
		s.port, err = bindEphemeral(s.router, endpoint)
		if err != nil {
			return err
		}

	case cmdPort:
		s.pipe <- &resp{method: cmdPort, payload: s.port}

	case cmdLoad:
		filename := msg.arg.(string)

		info, err := os.Stat(filename)
		if err != nil {
			return err
		}

		if s.configInfo != nil &&
			s.configInfo.ModTime() == info.ModTime() &&
			s.configInfo.Size() == info.Size() {
			// The config file hasn't been changed
			return nil
		}

		s.configInfo = info
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			return err
		}
		err = zpl.Unmarshal(data, &s.config)
		if err != nil {
			return err
		}

	case cmdSet:
		args := msg.arg.([]string)
		if len(args) < 2 {
			return errors.New("Not enough arguments for set command")
		}

		// path := args[0]
		// value := args[1]
		// TODO(armen): Implement the set command

	case cmdSave:
		// TODO(armen): Implement Save command

	default:
		r, err := s.method(msg)
		if err != nil {
			return err
		}
		s.pipe <- r
	}

	return nil
}

// Handle a protocol message from the client
func (s *server) handleProtocol() error {
	for e, err := s.router.GetEvents(); err == nil && e == zmq.POLLIN; {
		s.message, err = msg.Recv(s.router)
		if err != nil {
			return err
		}
		routeID := fmt.Sprintf("%x", s.message.RoutingID())

		if _, ok := s.clients[routeID]; !ok {
			s.clients[routeID] = s.newClient()
		}
		c := s.clients[routeID]
		// Any input from client counts as activity
		if c.ticket != 0 {
			s.loop.RemoveChannel(c.ticket)
			c.ticket = s.loop.AddChannelTime(time.Tick(s.timeout), 1, func(i interface{}) error { return c.handleTimeout() })
		}

		// Pass to client state machine
		c.execute()
	}

	return nil
}

// Watch server config file and reload if changed
func (s *server) watchConfig() error {
	return nil
}

// Reactor callback when client ticket expires
func (c *client) handleTimeout() error {
	return nil
}

// Execute state machine as long as we have events
func (c *client) execute() error {
	return nil
}

// Binds a zeromq socket to an ephemeral port and returns the port
func bindEphemeral(s *zmq.Socket, endpoint string) (port uint16, err error) {

	for i := dynPortFrom; i <= dynPortTo; i++ {
		rand.Seed(time.Now().UTC().UnixNano())
		port = uint16(rand.Intn(int(dynPortTo-dynPortFrom))) + dynPortFrom
		err = s.Bind(fmt.Sprintf("tcp://%s:%d", endpoint, port))
		if err == nil {
			break
		} else if i-dynPortFrom > 100 {
			err = errors.New("Unable to bind to an ephemeral port")
			break
		}
	}

	return port, err
}
