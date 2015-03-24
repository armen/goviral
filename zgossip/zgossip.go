// Package zgossip
package zgossip

// myServer defines the context for each running server. Store
// whatever properties and structures you need for the server.
// myServer will be embedded in engine's server struct.
type myServer struct {
	// TODO: Add any properties you need here
}

// myClient defines the state for each client connection. myClient
// will be embedded in engine's client struct. Note that all the actions
// are defined on client not this type but since this type is embedded
// in client they can access all the properties you defined here.
type myClient struct {
	// TODO: Add specific properties for your application
}

// Allocate properties and structures for a new server instance.
func (s *server) init() error {
	return nil
}

// Free properties and structures for a server instance
func (s *server) terminate() error {
	// Destroy properties here
	return nil
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
	return nil
}

// getNextTuple
func (c *client) getNextTuple() error {
	return nil
}

// storeTupleIfNew
func (c *client) storeTupleIfNew() error {
	return nil
}

// getTupleToForward
func (c *client) getTupleToForward() error {
	return nil
}
