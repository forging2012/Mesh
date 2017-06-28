package Managers

import (
	"errors"
	"sync"
	"time"

	balancers "github.com/eddytrex/Mesh/Balancers"
	mmg "github.com/eddytrex/Mesh/MeshMsg"
	peer "github.com/libp2p/go-libp2p-peer"
)

// BalancerMg save and manage all the balancers in the caller
type BalancerMg struct {
	sync.RWMutex
	Balancers        map[string]balancers.Balancer
	updateInterval   time.Duration
	stopUpdate       chan struct{}
	getNodes         func(ID string) []peer.ID
	metadataByPrefix map[string]func(ID string, newPeers []peer.ID) []*mmg.FnMetadata
	getMetadata      func(ID string, newPeers []peer.ID) []*mmg.FnMetadata
}

//NewBalancerMg create a new balancer Manager
func NewBalancerMg(getNodes func(ID string) []peer.ID, interval time.Duration) *BalancerMg {
	blMng := &BalancerMg{
		Balancers:        make(map[string]balancers.Balancer),
		updateInterval:   interval,
		stopUpdate:       make(chan struct{}),
		getNodes:         getNodes,
		metadataByPrefix: make(map[string]func(ID string, newPeers []peer.ID) []*mmg.FnMetadata),
	}
	blMng.RunUpdate()
	return blMng
}

//AddFnGetMetada add a metadata function to a pfrefix
func (bm *BalancerMg) AddFnGetMetada(prefix string, metadata func(ID string, newPeers []peer.ID) []*mmg.FnMetadata) {
	bm.metadataByPrefix[prefix] = metadata
}

//GetBalancer get the balancer of a specific funcion
func (bm *BalancerMg) GetBalancer(ID string) balancers.Balancer {
	bm.Lock()
	defer bm.Unlock()
	v, _ := bm.Balancers[ID]
	return v
}

//AddBalancer add a new balancer to a new ID, this rotate the new Balancer too
func (bm *BalancerMg) AddBalancer(ID string, nBalancer balancers.Balancer) error {
	peers := bm.getNodes(ID)

	prefix := ID[:4]
	getMetadata := bm.metadataByPrefix[prefix]

	if getMetadata == nil {
		return errors.New("There is not getMetadata function for this prefix: " + prefix)
	}

	getMetadataInBalancer := func(peers []peer.ID) []*mmg.FnMetadata {
		return getMetadata(ID, peers)
	}

	nBalancer.Rotate(peers, getMetadataInBalancer)

	bm.Lock()
	bm.Balancers[ID] = nBalancer
	bm.Unlock()
	return nil
}

//UpdateBalancer make and other search with fnID
func (bm *BalancerMg) UpdateBalancer(ID string) {
	bm.Lock()
	b, exist := bm.Balancers[ID]
	bm.Unlock()
	if exist {
		go func() {
			b.Lock()
			peers := bm.getNodes(ID)
			fnMedata := bm.metadataByPrefix[ID[:4]]

			getMetadataInBalancer := func(peers []peer.ID) []*mmg.FnMetadata {
				return fnMedata(ID, peers)
			}

			b.Rotate(peers, getMetadataInBalancer)
			b.Unlock()
		}()
	}
}

//Stop send a signal to stop the update of the balancer
func (bm *BalancerMg) Stop() {
	bm.stopUpdate <- struct{}{}
}

//RunUpdate update all the blancer every updateInterval
func (bm *BalancerMg) RunUpdate() {
	ticker := time.NewTicker(bm.updateInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				// maybe this is wrong?
				bm.Lock()
				balancers := bm.Balancers
				bm.Unlock()

				for key, vBalancer := range balancers {
					newPeers := bm.getNodes(key)
					fnMetadata := bm.metadataByPrefix[key[:4]]

					getMetadataInBalancer := func(peers []peer.ID) []*mmg.FnMetadata {
						return fnMetadata(key, newPeers)
					}

					vBalancer.Rotate(newPeers, getMetadataInBalancer)
				}
			case <-bm.stopUpdate:
				ticker.Stop()
				return
			}
		}
	}()
}
