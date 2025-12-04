package p2p

import (
	"encoding/gob"
	"io"
	// "fmt"
)

type Decoder interface {
	
	
	Decode(io.Reader, *RPC) error
}

type GOBDecoder struct {}

func (dec GOBDecoder) Decode(r io.Reader, msg *RPC) error {
	return gob.NewDecoder(r).Decode(msg)
}

type DefaultDecoder struct{}

// It takes data stream from incoming connection 
// and stores that in a buffer and then further stores 
// that buffered data to msg.Payload 
func (dec DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	peekBuf := make([]byte,1)
	if _,err := r.Read(peekBuf); err != nil {
		return nil
	}
	//In case of a stream we are not decoding what is being sent over the network
	// we are just setting stream true, so we can handle that in our logic.
	stream:= peekBuf[0] == IncomingStream
	if stream {
		msg.Stream = true
		return nil
	}

	buf := make([]byte,1028)
	n, err := r.Read(buf) // this blocks until data comes. 
	if err != nil {
		return err
	}
	msg.Payload = buf[:n]

	return nil
}