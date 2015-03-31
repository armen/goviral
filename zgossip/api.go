// api.go file is 100% generated. If you edit this file, you will lose
// your changes at the next build cycle.
// DO NOT MAKE ANY CHANGES YOU WISH TO KEEP.
//
// The correct places for commits are:
//  - The XML model used for this code generation: zgossip.xml
//  - The code generation script that built this file: zproto_server_go

package zgossip

import (
	"errors"
	"time"
)

type Zgossip struct {
	server *server
}

// New creates a new Zgossip instance.
func New(logPrefix string) (*Zgossip, error) {
	var err error

	z := &Zgossip{}
	z.server, err = newServer(logPrefix)
	if err != nil {
		return nil, err
	}

	go func() {
		z.server.loop.Run(pipeInterval / 10)

		z.server.destroy()
	}()

	return z, nil
}

// Resp returns the response channel. Resp channel should be used to
// receive responses from the server.
func (z *Zgossip) Resp() chan interface{} {
	return z.server.resp
}

// SendCmd sends a command to the server.
func (z *Zgossip) SendCmd(method string, arg interface{}, timeout time.Duration) error {

	select {
	case z.server.pipe <- &cmd{method: method, arg: arg}:
	case <-time.Tick(timeout):
		return errors.New("Unable to send the command to the engine")
	}

	return nil
}

// RecvResp returns a response from the pipe channel (the server).
func (z *Zgossip) RecvResp(timeout time.Duration) (interface{}, error) {

	select {
	case m := <-z.server.pipe:
		r := m.(*Resp)
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Payload, nil

	case <-time.Tick(timeout):
		return nil, errors.New("Unable to receive a response from the engine")
	}

	return nil, nil
}

// Run starts the server and waits for connections. Note that this is blocking
// and will wait until a signal is received or a handler returns with an error.
func (z *Zgossip) Run() error {
	return z.server.loop.Run(pipeInterval / 5)
}
