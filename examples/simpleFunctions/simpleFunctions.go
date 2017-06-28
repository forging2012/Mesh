package main

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/eddytrex/Mesh"
	protocol "github.com/eddytrex/Mesh/examples/protocol"
	protob "github.com/gogo/protobuf/proto"
	golog "github.com/ipfs/go-log"
	gologging "github.com/whyrusleeping/go-logging"
)

func f1(x protob.Message) (protob.Message, error) {
	r1 := x.(*protocol.X)
	senderror := rand.Int() % 2
	if senderror == 0 {
		return nil, errors.New("I am an error: " + r1.Msg)
	}
	res := protocol.FnX{
		MsgR: "I am an output: " + r1.Msg,
	}
	return protob.Message(&res), nil
}

func main() {
	golog.SetAllLoggers(gologging.ERROR)
	nodes := make([]Mesh.Node, 1, 1)
	node0 := Mesh.NewNode(9200)
	nodes[0] = *node0
	addressPublic := node0.GetAddresses()

	for i := 1; i <= 10; i++ {
		nodei := Mesh.NewNode(9200+i, addressPublic[0])
		bError := nodei.BootstrapDHT(5 * time.Second)
		if bError == nil {
			installService := rand.Int() % 2
			if installService == 0 {
				nodei.PublishFunction("f1", f1, &protocol.X{}, &protocol.FnX{})
			}

			nodes = append(nodes, *nodei)
		}
	}

	fmt.Printf("Call:")
	nodeNplus1 := Mesh.NewNode(9301, addressPublic[0])
	nodeNplus1.BootstrapDHT(5 * time.Second)
	fn := nodeNplus1.GetPublishedFunction("f1", &protocol.X{}, &protocol.FnX{})

	for i := 0; i < 20; i++ {
		ret, err := fn(&protocol.X{Msg: "TEST TEST" + strconv.Itoa(i)})
		if err != nil {
			fmt.Println("Error:", err, " Result:", ret)
			continue
		}
		FnX := ret.(*protocol.FnX)
		fmt.Println("Result:", FnX.MsgR)
	}

	select {}
}
