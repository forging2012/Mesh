package QueueClients

import (
	"errors"

	msg "github.com/eddytrex/Mesh/MeshMsg"
)

//GetClitByMetadata a function to map the qType to a specific client
func GetClitByMetadata(qType string, metadata string) (QueueClient, error) {

	switch qType {
	case "Redis":
		return NewRedisClient(metadata), nil
	case "RabbitMq":
		return NewRabbitMq(metadata), nil
	default:
		return nil, errors.New("Queue Client Type Not found ")
	}
}

//GetFnMetadataByCli function to map a cli to a FnMetadata
func GetFnMetadataByCli(cli QueueClient, peerID string) (*msg.FnMetadata, error) {

	switch cli.(type) {
	case *Redis:

		return &msg.FnMetadata{
			Metadata:     cli.GetStrConnection(),
			QueueClientN: "Redis",
			PeerID:       peerID,
		}, nil
	case *RabbitMq:
		return &msg.FnMetadata{

			Metadata:     cli.GetStrConnection(),
			QueueClientN: "RabbitMq",
			PeerID:       peerID,
		}, nil
	default:
		return nil, errors.New("Queue Client Type Not found ")
	}

}
