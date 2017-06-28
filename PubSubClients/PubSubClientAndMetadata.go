package PubSubClients

import (
	"errors"

	msg "github.com/eddytrex/Mesh/MeshMsg"
	flSub "github.com/libp2p/go-floodsub"
)

//GetClitByMetadata a function to map the qType to a specific client
func GetClitByMetadata(m *msg.FnMetadata, flcli *flSub.PubSub) (PubSubCli, error) {

	switch m.GetPubSubClient() {
	case "Redis":
		return NewRedisCli(m.GetMetadata()), nil
	case "RabbitMq":
		return NewRabbitMqCli(m.GetMetadata()), nil
	case "FloodSub":

		return NewFloodSubCli(flcli), nil
	default:
		return nil, errors.New("Client Type Not found ")
	}
}

//GetFnMetadataByCli function to map a cli to a FnMetadata
func GetFnMetadataByCli(cli PubSubCli, peerID string) (*msg.FnMetadata, error) {

	switch cli.(type) {
	case *RedisCli:
		return &msg.FnMetadata{
			Metadata:     cli.StrConnection(),
			PubSubClient: "Redis",
			PeerID:       peerID,
		}, nil
	case *RabbitMqCli:
		return &msg.FnMetadata{
			Metadata:     cli.StrConnection(),
			PubSubClient: "RabbitMq",
			PeerID:       peerID,
		}, nil
	case *FloodSubCli:
		return &msg.FnMetadata{
			Metadata:     cli.StrConnection(),
			PubSubClient: "FloodSub",
			PeerID:       peerID,
		}, nil
	default:
		return nil, errors.New("Queue Client Type Not found ")
	}

}
