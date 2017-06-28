package Balancers

import (
	"sync"

	msg "github.com/eddytrex/Mesh/MeshMsg"
	peer "github.com/libp2p/go-libp2p-peer"
)

//Balancer define Software balancer used by the caller
type Balancer interface {
	sync.Locker
	Next() (*msg.FnMetadata, error)
	Rotate(newPeers []peer.ID, getMetadata func(peers []peer.ID) []*msg.FnMetadata)
	RecordMetric(peer peer.ID, attribute string, value float64)
	//Mark(*msg.FnMetadata)??? to save the unusable registers
}
