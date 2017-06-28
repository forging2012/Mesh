package QueueClients

import (
	"errors"
	"log"
	"sync"

	msg "github.com/eddytrex/Mesh/MeshMsg"
)

//PublisherQueueClients represent all clients by its metadata used in a balancer
type PublisherQueueClients struct {
	publisherCli map[string]QueueClient
	muxCli       sync.RWMutex
}

//NewPublisherClients create a new struct PublisherClients
func NewPublisherClients() *PublisherQueueClients {
	return &PublisherQueueClients{
		publisherCli: make(map[string]QueueClient),
		muxCli:       sync.RWMutex{},
	}
}

//AddClient Add new Clinet with the metadata
func (pc *PublisherQueueClients) AddClient(fnmetadata *msg.FnMetadata) {
	metadata := fnmetadata.GetMetadata()
	pc.muxCli.RLock()
	_, v := pc.publisherCli[metadata]
	pc.muxCli.RUnlock()

	if !v {

		cli, er := GetClitByMetadata(fnmetadata.GetQueueClientN(), metadata)
		if er != nil {
			log.Printf("Error on add queue client %s", er)
			return
		}

		pc.muxCli.Lock()
		pc.publisherCli[metadata] = cli
		pc.muxCli.Unlock()
	}

}

//GetClient get client by its metadata
func (pc *PublisherQueueClients) GetClient(metadata string) QueueClient {
	pc.muxCli.RLock()
	result := pc.publisherCli[metadata]
	pc.muxCli.RUnlock()
	return result
}

//GetClientLessOverHeadClient get the clinet with less length in the queue (qName)
func (pc *PublisherQueueClients) GetClientLessOverHeadClient(qName string) QueueClient {
	pc.muxCli.RLock()
	var result QueueClient
	var vResult int64
	first := true

	for _, client := range pc.publisherCli {
		vClient, err := client.GetLenght(qName)
		if err != nil {
			log.Printf(" Could not get the Lenght of the queue %s. Client get the error: %s", qName, err)
			continue
		}
		if first {
			result = client
			vResult = vClient
			first = false
		} else {
			if vClient < vResult {
				result = client
				vResult = vClient
			}
		}
	}
	if result == nil {
		er := errors.New(" Could not connect to any queue for: " + qName)
		log.Printf("%s", er)
		//panic(er) //??
	}
	pc.muxCli.RUnlock()
	return result
}
