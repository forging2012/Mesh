package Balancers

import (
	"sync"

	"time"

	mmg "github.com/eddytrex/Mesh/MeshMsg"
	qCli "github.com/eddytrex/Mesh/QueueClients"
	peer "github.com/libp2p/go-libp2p-peer"
)

//SimpleQueueBalancer a balancer used to publish in the differents queues
type SimpleQueueBalancer struct {
	*sync.Mutex
	QName             string
	Clients           *qCli.PublisherQueueClients
	nextStrConnection string
}

//NewSimpleQueueBalancer Create a new Balancer with clients
func NewSimpleQueueBalancer(clients *qCli.PublisherQueueClients, qName string, timeToCheckOverHead time.Duration) *SimpleQueueBalancer {
	result := &SimpleQueueBalancer{
		Mutex:             new(sync.Mutex),
		Clients:           clients,
		nextStrConnection: "",
		QName:             qName,
	}
	result.netxWithLessOverHead(timeToCheckOverHead)
	return result
}

func (q *SimpleQueueBalancer) netxWithLessOverHead(timeToCheckOverHead time.Duration) {
	ticker := time.NewTicker(timeToCheckOverHead)
	go func() {
		for {
			select {
			case <-ticker.C:
				q.Lock()
				if v := q.Clients.GetClientLessOverHeadClient(q.QName); v != nil {
					q.nextStrConnection = v.GetStrConnection()
				}
				q.Unlock()
			}

		}
	}()
}

//Next get the next queue to publish in this case the queue with less msg
func (q *SimpleQueueBalancer) Next() (*mmg.FnMetadata, error) {
	q.Lock()
	nextStr := q.nextStrConnection
	q.Unlock()

	next := mmg.FnMetadata{}

	if nextStr != "" {
		next.Metadata = nextStr
	} else {
		q.Lock()
		if v := q.Clients.GetClientLessOverHeadClient(q.QName); v != nil {
			q.nextStrConnection = v.GetStrConnection()
			next.Metadata = q.nextStrConnection
		}
		q.Unlock()
	}
	return &next, nil
}

//Rotate renew the queues using getMetada function
func (q *SimpleQueueBalancer) Rotate(newPeers []peer.ID, getMetadata func(peers []peer.ID) []*mmg.FnMetadata) {
	metadatPeers := getMetadata(newPeers)
	for _, iMetadataPeer := range metadatPeers {
		q.Clients.AddClient(iMetadataPeer)
	}
}

//RecordMetric Save the metric to a especific peer id
func (q *SimpleQueueBalancer) RecordMetric(peer peer.ID, attribute string, value float64) {}
