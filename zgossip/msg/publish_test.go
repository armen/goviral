package msg

import (
	"testing"

	zmq "github.com/pebbe/zmq4"
)

// Yay! Test function.
func TestPublish(t *testing.T) {

	// Create pair of sockets we can send through

	// Output socket
	output, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()

	routingID := "Shout"
	output.SetIdentity(routingID)
	err = output.Bind("inproc://selftest-publish")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Unbind("inproc://selftest-publish")

	// Input socket
	input, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	err = input.Connect("inproc://selftest-publish")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Disconnect("inproc://selftest-publish")

	// Create a Publish message and send it through the wire
	publish := NewPublish()

	publish.Key = "Life is short but Now lasts for ever"

	publish.Value = "Life is short but Now lasts for ever"

	publish.Ttl = 123

	err = publish.Send(output)
	if err != nil {
		t.Fatal(err)
	}
	transit, err := Recv(input)
	if err != nil {
		t.Fatal(err)
	}

	tr := transit.(*Publish)

	if tr.Key != "Life is short but Now lasts for ever" {
		t.Fatalf("expected %s, got %s", "Life is short but Now lasts for ever", tr.Key)
	}

	if tr.Value != "Life is short but Now lasts for ever" {
		t.Fatalf("expected %s, got %s", "Life is short but Now lasts for ever", tr.Value)
	}

	if tr.Ttl != 123 {
		t.Fatalf("expected %d, got %d", 123, tr.Ttl)
	}

	err = tr.Send(input)
	if err != nil {
		t.Fatal(err)
	}

	transit, err = Recv(output)
	if err != nil {
		t.Fatal(err)
	}

	if routingID != string(tr.RoutingID()) {
		t.Fatalf("expected %s, got %s", routingID, string(tr.RoutingID()))
	}
}
