package Managers

import (
	"fmt"
	"reflect"
	"sync"

	msg "github.com/eddytrex/Mesh/MeshMsg"
	"github.com/eddytrex/Mesh/PubSubClients"
	protob "github.com/gogo/protobuf/proto"
)

type PubSubMg struct {
	subscriptions map[string]*PubSubClients.Subscription
	m             *sync.Mutex
}

func NewPubSubMng() *PubSubMg {
	return &PubSubMg{
		m:             &sync.Mutex{},
		subscriptions: make(map[string]*PubSubClients.Subscription),
	}
}

func (psm *PubSubMg) Subscribe(nodeId string, ChID string, dummyType reflect.Type, cli PubSubClients.PubSubCli) (s *PubSubClients.Subscription, err error) {

	psm.m.Lock()
	sub, exist := psm.subscriptions[ChID]
	psm.m.Unlock()

	if exist {
		return sub, nil
	}
	subscription, err := cli.Subscribe(ChID)

	if err != nil {
		return nil, err
	}

	marshalAndPublish := func(pub chan protob.Message, stop chan struct{}) {
		for {
			select {
			case newMsg := <-pub:
				mByte, errM := protob.Marshal(newMsg)
				if errM != nil {
					//log
					continue
				}

				//warp
				newWrap := &msg.WrapPubSub{
					Msg:    mByte,
					NodeId: nodeId,
				}

				wrapBytes, errWrap := protob.Marshal(newWrap)
				if errWrap != nil {
					//log
					continue
				}

				//Finaly Publish
				errPub := subscription.PublishMsg(wrapBytes)
				if errPub != nil {
					fmt.Println("->", errPub)
					//log
				}
			case <-stop:
				return
			}
		}
	}

	getMsgAndUnmarshal := func(fanout func(x PubSubClients.WrapMsg), stop chan struct{}) {
		for {
			select {
			case <-stop:
				return
			default:
				bMsg, err := subscription.NextMsg()
				if err != nil {
					//log
					continue
				}
				//unwrap
				newWrap := &msg.WrapPubSub{}
				errUnWrap := protob.Unmarshal(bMsg, newWrap)

				if errUnWrap != nil {
					//log
					continue
				}

				newMsg := reflect.New(dummyType).Interface().(protob.Message)
				errUnmarshall := protob.Unmarshal(newWrap.Msg, newMsg)

				if errUnmarshall != nil {
					//log
					continue
				}

				newMessage := PubSubClients.WrapMsg{
					ID:  newWrap.NodeId,
					Msg: newMsg,
				}

				fanout(newMessage)
			}
		}
	}

	newSub := PubSubClients.NewSubscription(marshalAndPublish, getMsgAndUnmarshal, subscription.UnSubscribe)

	psm.m.Lock()
	psm.subscriptions[ChID] = newSub
	psm.m.Unlock()

	return newSub, nil
}
