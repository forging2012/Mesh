package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/eddytrex/Mesh"
	config "github.com/eddytrex/Mesh/Config"
	qcli "github.com/eddytrex/Mesh/QueueClients"
	protocol "github.com/eddytrex/Mesh/examples/protocol"
	protob "github.com/gogo/protobuf/proto"
	golog "github.com/ipfs/go-log"
	gologging "github.com/whyrusleeping/go-logging"
)

func getQueueClient(i int, clients []qcli.QueueClient) qcli.QueueClient {
	if i == 0 {
		return clients[0]
	}
	return clients[1]
}

func main() {

	consumerGenerator := func(i int) func(x protob.Message) error {
		stri := strconv.Itoa(i)
		return func(x protob.Message) error {
			r1 := x.(*protocol.X)
			fmt.Printf("Consuming something: %s in the consumer %s\n", r1.GetMsg(), stri)
			return nil
		}
	}

	golog.SetAllLoggers(gologging.ERROR)
	nodes := make([]Mesh.Node, 1, 1)
	node0 := Mesh.NewNode(9200)
	nodes[0] = *node0
	addressPublic := node0.GetAddresses()

	clis := make([]qcli.QueueClient, 0, 0)
	clis = append(clis, qcli.NewRedisClient("localhost:6379"))
	clis = append(clis, qcli.NewRabbitMq("amqp://guest:guest@localhost:5672"))

	config := config.ConsumerConfig{
		Retry:       2,
		ConsumeDL:   true,
		CheckDL:     5 * time.Second,
		NoGorutines: 5,
	}

	for i := 1; i <= 2; i++ {
		senderror := rand.Int() % 2
		sCli := getQueueClient(senderror, clis)

		nodei := Mesh.NewNode(9200+i, addressPublic[0])
		nodei.BootstrapDHT(5 * time.Second)

		consumeri := consumerGenerator(i)
		erInAdC := nodei.AddConsumer("queue1", consumeri, &protocol.X{}, config, sCli)
		if erInAdC != nil {
			fmt.Println("error in add consumer", erInAdC)
		}

	}

	fmt.Println("Publisher:")
	nodeNplus1 := Mesh.NewNode(9301, addressPublic[0])
	nodeNplus1.BootstrapDHT(5 * time.Second)
	publisher := nodeNplus1.PublisherFunction("queue1", &protocol.X{})

	for i := 1; i <= 500; i++ {
		publisher(&protocol.X{Msg: "MSG MSG" + strconv.Itoa(i)})
		//fmt.Println("->", erPublish)
	}

	select {}
}
