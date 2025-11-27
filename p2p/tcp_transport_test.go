package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	opts := TCPtransportOpts{
		ListenAddr:    ":3000",
		HandshakeFunc: NOPHandshakefunc,
		Decoder:       DefaultDecoder{},
	}
	tr := NewTCPTransport(opts)
	assert.Equal(t, tr.ListenAddr, ":3000")

	//server

	assert.Nil(t, tr.ListenAndAccept())
}
