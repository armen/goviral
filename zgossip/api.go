// Package zgossip is 100% generated. If you edit this file,
// you will lose your changes at the next build cycle.
// DO NOT MAKE ANY CHANGES YOU WISH TO KEEP.
//
// The correct places for commits are:
//  - The XML model used for this code generation: zgossip.xml
//  - The code generation script that built this file: zproto_server_go
package zgossip

type Zgossip struct {
	server *server
}

func New() (*Zgossip, error) {
	var err error

	z := &Zgossip{}
	z.server, err = newServer()
	if err != nil {
		return nil, err
	}

	return z, nil
}

func (z *Zgossip) Run() error {
	return z.server.loop.Run(pipeInterval / 5)
}
