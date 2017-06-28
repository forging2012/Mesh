package main

import (
	"fmt"
	"math/rand"
	"time"

	"strconv"

	"github.com/eddytrex/Mesh"
	PubSubCli "github.com/eddytrex/Mesh/PubSubClients"
	protocol "github.com/eddytrex/Mesh/examples/protocol"
	protob "github.com/gogo/protobuf/proto"
	golog "github.com/ipfs/go-log"
	gologging "github.com/whyrusleeping/go-logging"
)

func consumer(nConsumer string, newMsg <-chan PubSubCli.WrapMsg) {
	for m := range newMsg {
		rm := m.Msg.(*protocol.X)
		fmt.Println("Consumer", nConsumer, ":", rm.GetMsg())
	}
}

func producer(n chan<- protob.Message) {
	for i := 0; i <= 20; i++ {
		nx := &protocol.X{
			Msg: "New Message No." + strconv.Itoa(i),
		}
		n <- nx
	}
}

func main() {
	golog.SetAllLoggers(gologging.ERROR)
	nodes := make([]Mesh.Node, 2, 2)
	node0 := Mesh.NewNode(9200)
	nodes[0] = *node0
	addressPublic := node0.GetAddresses()
	node0.BootstrapDHT(5 * time.Second)

	nodei := Mesh.NewNode(9200+1, addressPublic[0])
	nodes[1] = *nodei
	nodei.BootstrapDHT(5 * time.Second)

	//sip, eCreateTopic := nodei.CreateTopic("TestTopic", &protocol.X{}, PubSubCli.NewRedisCli("localhost:6379"))
	sip, eCreateTopic := nodei.CreateTopic("TestTopic", &protocol.X{}, PubSubCli.NewRabbitMqCli("amqp://guest:guest@localhost:5672"))
	//sip, eCreateTopic := nodei.CreateTopic("TestTopic", &protocol.X{}, PubSubCli.NewFloodSubCli(nodei.GetFlCli())) //maybe is not good idea..

	if eCreateTopic != nil {
		return
	}

	for i := 2; i <= 6; i++ {
		nodei := Mesh.NewNode(9200+i, addressPublic[0])
		bError := nodei.BootstrapDHT(5 * time.Second)
		if bError == nil {
			installService := rand.Int() % 2
			if installService == 0 {
				si, eJoinTopci := nodei.JoinToTopic("TestTopic", &protocol.X{})
				if eJoinTopci != nil {
					return
				}
				go consumer(strconv.Itoa(i), si.GetNewMessagesChannel())
			}
			nodes = append(nodes, *nodei)
		}
	}

	fmt.Println("Produce ")
	producer(sip.GetPublisherChannel())
	select {}
}
