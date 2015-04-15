// engine.go file is 100% generated. If you edit this file, you will lose
// your changes at the next build cycle.
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
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
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

	// Port range 0xc000~0xffff is defined by IANA for dynamic or private ports
	// We use this when choosing a port for dynamic binding
	dynPortFrom uint16 = 0xc000
	dynPortTo   uint16 = 0xffff
)

// Names for state machine logging and error reporting
var stateName = []string{
	"(NONE)",
	"start",
	"have tuple",
	"connected",
	"external",
}

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

type Resp struct {
	Method  string
	Payload interface{}
	Err     error
}

// Context for the whole server task. This embeds the application-level
// server context so we can access to methods and properties defined in
// application-level server.
type server struct {
	myServer // Application-level server context

	pipe           chan interface{}       // Channel to back to caller API
	resp           chan interface{}       // Channel to back to caller API (For responses)
	router         *zmq.Socket            // Socket to talk to clients
	routerEndpoint string                 // Router endpoint
	port           uint16                 // Server port bound to
	loop           *zmq.Reactor           // Reactor for server sockets
	message        msg.Transit            // Message in and out
	clients        map[string]*client     // Clients we're connected to
	config         map[string]interface{} // Configuration tree
	configInfo     os.FileInfo            // Configuration file info
	clientID       uint                   // Client identifier counter
	timeout        time.Duration          // Default client expiry timeout
	verbose        bool                   // Verbose logging enabled?
	logPrefix      string                 // Default log prefix
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

func newServer(logPrefix string) (*server, error) {
	s := &server{
		pipe:      make(chan interface{}),
		resp:      make(chan interface{}, 10), // Response channel is a buffered channel
		loop:      zmq.NewReactor(),
		clients:   make(map[string]*client),
		config:    make(map[string]interface{}),
		logPrefix: logPrefix,
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

	s.addChanHandler(s.pipe, func(msg interface{}) error { return s.handlePipe(msg) })
	s.addTicker(time.Tick(confInterval), func(i interface{}) error { return s.watchConfig() })

	rand.Seed(time.Now().UTC().UnixNano())
	s.clientID = uint(rand.Intn(1000))

	// Initialize application server context
	err = s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) destroy() {
	for _, client := range s.clients {
		client.destroy()
	}
	s.terminate()
	if s.routerEndpoint != "" {
		s.router.Unbind(s.routerEndpoint)
		s.router.Close()
	}
}

// Execute 'event' on all clients known to the server
func (s *server) broadcastEvent(e event) {
	for _, client := range s.clients {
		client.execute(e)
	}
}

// Adds the socket to the loop
func (s *server) addSocketHandler(socket *zmq.Socket, events zmq.State, handler func(zmq.State) error) {
	s.loop.AddSocket(socket, events, handler)
}

// Removes the socket from the loop
func (s *server) removeSocketHandler(socket *zmq.Socket) {
	s.loop.RemoveSocket(socket)
}

// Adds the channel to the loop
func (s *server) addChanHandler(ch <-chan interface{}, handler func(interface{}) error) uint64 {
	return s.loop.AddChannel(ch, 1, handler)
}

// Removes the channel from the loop
func (s *server) removeChanHandler(id uint64) {
	s.loop.RemoveChannel(id)
}

// Adds the ticker to the loop
func (s *server) addTicker(ch <-chan time.Time, handler func(interface{}) error) uint64 {
	return s.loop.AddChannelTime(ch, 1, handler)
}

// Removes the ticker from the loop
func (s *server) removeTicker(id uint64) {
	s.loop.RemoveChannel(id)
}

// Creates a new client
func (s *server) newClient() *client {

	s.clientID++

	c := &client{
		server:    s,
		routingID: s.message.RoutingID(),
		hashKey:   fmt.Sprintf("%x", s.message.RoutingID()),
		uniqueID:  s.clientID,
		logPrefix: fmt.Sprintf("%d:%s", s.clientID, s.logPrefix),
	}

	// If expiry timers are being used, create client timeout handler
	if s.timeout != 0 {
		c.ticket = s.addTicker(time.Tick(s.timeout), func(i interface{}) error { return c.handleTimeout() })
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
		// Shutdown the engine by sending an error to the reactor
		return errors.New("Terminating")

	case cmdBind:
		s.routerEndpoint = msg.arg.(string)
		s.port, err = bind(s.router, s.routerEndpoint)
		if err != nil {
			if s.verbose {
				log.Println(err)
			}
			return nil
		}

		s.addSocketHandler(s.router, zmq.POLLIN, func(e zmq.State) error { return s.handleProtocol() })

	case cmdPort:
		select {
		case s.pipe <- &Resp{Method: cmdPort, Payload: s.port}:
		case <-time.Tick(100 * time.Millisecond):
		}

	case cmdLoad:
		filename := msg.arg.(string)

		info, err := os.Stat(filename)
		if err != nil {
			if s.verbose {
				log.Println(err)
			}
			return nil
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
			if s.verbose {
				log.Println(err)
			}
			return nil
		}
		err = zpl.Unmarshal(data, &s.config)
		if err != nil {
			if s.verbose {
				log.Println(err)
			}
			return nil
		}

	case cmdSet:
		args := msg.arg.([]string)
		if len(args) < 2 {
			if s.verbose {
				log.Println("Not enough arguments for set command")
			}
			return nil
		}

		// path := args[0]
		// value := args[1]
		// TODO(armen): Implement the set command

	case cmdSave:
		// TODO(armen): Implement Save command

	default:
		r, err := s.method(msg)
		if err != nil {
			if s.verbose {
				log.Println(err)
			}
			return nil
		}
		if r != nil {
			select {
			case s.resp <- r:
			default:
			}
		}
	}

	return nil
}

// Handle a protocol message from the client
func (s *server) handleProtocol() (err error) {
	s.message, err = msg.Recv(s.router)
	if err != nil {
		return err
	}
	routingID := fmt.Sprintf("%x", s.message.RoutingID())

	if _, ok := s.clients[routingID]; !ok {
		s.clients[routingID] = s.newClient()
	}
	c := s.clients[routingID]

	// Any input from client counts as activity
	if c.ticket != 0 {
		c.server.removeTicker(c.ticket)
		c.ticket = s.addTicker(time.Tick(s.timeout), func(i interface{}) error { return c.handleTimeout() })
	}

	// Pass to client state machine
	c.execute(c.protocolEvent())

	return nil
}

// Watch server config file and reload if changed
func (s *server) watchConfig() error {
	return nil
}

// Reactor callback when client ticket expires
func (c *client) handleTimeout() error {
	c.execute(expiredEvent)
	c.server.removeTicker(c.ticket)
	c.ticket = 0
	return nil
}

func (c *client) protocolEvent() event {
	switch c.server.message.(type) {
	case *msg.Hello:
		return helloEvent
	case *msg.Publish:
		return publishEvent
	case *msg.Ping:
		return pingEvent
	default:
		// Invalid msg message
		return terminateEvent
	}
}

// Execute state machine as long as we have events
func (c *client) execute(e event) error {
	c.nextEvent = e
	// TODO(armen): Cancel wakeup timer, if any was pending

	for c.nextEvent > nullEvent {
		c.event = c.nextEvent
		c.nextEvent = nullEvent
		c.exception = nullEvent

		if c.server.verbose {
			log.Printf("%s: %s:", c.logPrefix, stateName[c.state])
			log.Printf("%s:     %s", c.logPrefix, eventName[c.event])
		}

		switch c.state {
		case startState:
			switch c.event {
			case helloEvent:
				if c.exception == 0 {
					// get first tuple
					if c.server.verbose {
						log.Printf("%s:         $ get first tuple", c.logPrefix)
					}

					if err := c.getFirstTuple(); err != nil {
						return err
					}
				}
				if c.exception == 0 {
					c.state = haveTupleState
				}
			case pingEvent:
				if c.exception == 0 {
					// send Pong
					if c.server.verbose {
						log.Printf("%s:         $ send Pong", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewPong()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
			case expiredEvent:
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			default:
				// Handle unexpected protocol events
				if c.exception == 0 {
					// send Invalid
					if c.server.verbose {
						log.Printf("%s:         $ send Invalid", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewInvalid()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			}
		case haveTupleState:
			switch c.event {
			case okEvent:
				if c.exception == 0 {
					// send Publish
					if c.server.verbose {
						log.Printf("%s:         $ send Publish", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewPublish()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
				if c.exception == 0 {
					// get next tuple
					if c.server.verbose {
						log.Printf("%s:         $ get next tuple", c.logPrefix)
					}

					if err := c.getNextTuple(); err != nil {
						return err
					}
				}
			case finishedEvent:
				if c.exception == 0 {
					c.state = connectedState
				}
				if c.exception != 0 {
					// Handle unexpected internal events
					log.Printf("%s: unhandled event %s in %s", c.logPrefix, eventName[c.event], stateName[c.state])
				}
			}
		case connectedState:
			switch c.event {
			case publishEvent:
				if c.exception == 0 {
					// store tuple if new
					if c.server.verbose {
						log.Printf("%s:         $ store tuple if new", c.logPrefix)
					}

					if err := c.storeTupleIfNew(); err != nil {
						return err
					}
				}
			case forwardEvent:
				if c.exception == 0 {
					// get tuple to forward
					if c.server.verbose {
						log.Printf("%s:         $ get tuple to forward", c.logPrefix)
					}

					if err := c.getTupleToForward(); err != nil {
						return err
					}
				}
				if c.exception == 0 {
					// send Publish
					if c.server.verbose {
						log.Printf("%s:         $ send Publish", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewPublish()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
			case pingEvent:
				if c.exception == 0 {
					// send Pong
					if c.server.verbose {
						log.Printf("%s:         $ send Pong", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewPong()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
			case expiredEvent:
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			default:
				// Handle unexpected protocol events
				if c.exception == 0 {
					// send Invalid
					if c.server.verbose {
						log.Printf("%s:         $ send Invalid", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewInvalid()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			}
		case externalState:
			switch c.event {
			case pingEvent:
				if c.exception == 0 {
					// send Pong
					if c.server.verbose {
						log.Printf("%s:         $ send Pong", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewPong()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
			case expiredEvent:
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			default:
				// Handle unexpected protocol events
				if c.exception == 0 {
					// send Invalid
					if c.server.verbose {
						log.Printf("%s:         $ send Invalid", c.logPrefix)
					}
					// Set the message if it's not already set by previous actions
					if c.server.message == nil {
						c.server.message = msg.NewInvalid()
					}
					c.server.message.SetRoutingID(c.routingID)
					if err := c.server.message.Send(c.server.router); err != nil {
						c.server.message = nil
						return err
					}
					c.server.message = nil
				}
				if c.exception == 0 {
					// terminate
					if c.server.verbose {
						log.Printf("%s:         $ terminate", c.logPrefix)
					}
					c.nextEvent = terminateEvent
				}
			}
		}
		// If we had an exception event, interrupt normal programming
		if c.exception != 0 {
			if c.server.verbose {
				log.Printf("%s:         ! %s", c.logPrefix, eventName[c.exception])
			}

			c.nextEvent = c.exception
		}
		if c.nextEvent == terminateEvent {
			c.server.clients[c.hashKey].destroy()
			delete(c.server.clients, c.hashKey)
			break
		} else if c.server.verbose {
			log.Printf("%s:         > %s", c.logPrefix, stateName[c.state])
		}
	}

	return nil
}

func (c *client) destroy() {
	if c.ticket != 0 {
		c.server.removeTicker(c.ticket)
		c.ticket = 0
	}

	c.logPrefix = "*** TERMINATED ***"
	c.terminate()
}

// Set the next event, needed in at least one action in an internal
// state; otherwise the state machine will wait for a message on the
// router socket and treat that as the event.
func (c *client) setNextEvent(e event) {
	c.nextEvent = e
}

// Binds a zeromq socket to an ephemeral port and returns the port
// If the port number is specified in the endpoint returns the specified
// port number and if port number isn't supported for the endpoint
// returns 0 for the port number.
func bind(s *zmq.Socket, endpoint string) (port uint16, err error) {

	e, err := url.Parse(endpoint)
	if err != nil {
		return 0, err
	}

	if e.Scheme == "inproc" {
		err = s.Bind(endpoint)
		return 0, err
	}
	_, p, err := net.SplitHostPort(e.Host)
	if err != nil {
		return 0, err
	}

	if p == "*" {
		for i := dynPortFrom; i <= dynPortTo; i++ {
			rand.Seed(time.Now().UTC().UnixNano())
			port = uint16(rand.Intn(int(dynPortTo-dynPortFrom))) + dynPortFrom
			endpoint = fmt.Sprintf("%s:%d", endpoint, port)
			err = s.Bind(endpoint)
			if err == nil {
				break
			} else if err.Error() == "no such device" {
				break
			} else if i-dynPortFrom > 100 {
				err = errors.New("Unable to bind to an ephemeral port")
				break
			}
		}

		return port, err
	}

	pp, err := strconv.ParseUint(p, 10, 16)
	if err != nil {
		return 0, err
	}
	port = uint16(pp)
	err = s.Bind(endpoint)

	return port, err
}
