package Balancers

import (
	"errors"
	"sync"

	mms "github.com/eddytrex/Mesh/MeshMsg"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
)

var errNoPeers = errors.New("There is no Peers in the balancer")

//BalancerRoundrobin define an implementation of balancer
type BalancerRoundrobin struct {
	*sync.Mutex
	pstore.Metrics
	peers []*mms.FnMetadata
	in    int
}

//NewBalancerRoundRobin create a new blancer RoundRobin
func NewBalancerRoundRobin() *BalancerRoundrobin {
	result := &BalancerRoundrobin{
		Mutex: new(sync.Mutex),
		peers: make([]*mms.FnMetadata, 0, 0),
		in:    0,
	}
	return result
}

//Next get de next peer to call
func (b *BalancerRoundrobin) Next() (*mms.FnMetadata, error) {
	b.Lock()
	defer b.Unlock()
	if len(b.peers) > 0 {
		b.in = b.in + 1
		if b.in >= len(b.peers) {
			b.in = 0
		}
		result := b.peers[b.in]
		return result, nil
	}
	return nil, errNoPeers
}

//Rotate renew  the peers in the balancer
func (b *BalancerRoundrobin) Rotate(newPeers []peer.ID, getMetadata func(peers []peer.ID) []*mms.FnMetadata) {
	if newPeers == nil || len(newPeers) == 0 {
		return
	}
	nPeers := getMetadata(newPeers)
	b.Lock()
	b.peers = nPeers
	b.Unlock()
}

// RecordMetric recrod a metric to a peer
func (b *BalancerRoundrobin) RecordMetric(peer peer.ID, attribute string, value float64) {}
