package p2p

// HandshakeFunc... ?
type HandshakeFunc func(Peer) error

func NOPHandshakefunc(Peer) error {
	return nil
}
