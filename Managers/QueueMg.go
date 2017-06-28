package Managers

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"

	b "github.com/eddytrex/Mesh/Balancers"
	config "github.com/eddytrex/Mesh/Config"
	ids "github.com/eddytrex/Mesh/IDs"
	mgs "github.com/eddytrex/Mesh/MeshMsg"
	queueClis "github.com/eddytrex/Mesh/QueueClients"
	protob "github.com/gogo/protobuf/proto"
)

// QueueMg save the stop Channles of the consumers
type QueueMg struct {
	stopConsumeCh map[string][]chan struct{}
	mux           sync.Mutex

	PublisherCli *queueClis.PublisherQueueClients
}

//NewQueuMg generate a new queueMsg
func NewQueuMg(pc *queueClis.PublisherQueueClients) *QueueMg {
	return &QueueMg{
		stopConsumeCh: make(map[string][]chan struct{}),
		mux:           sync.Mutex{},
		PublisherCli:  pc,
	}
}

//AddConsumer add a cnsumer with the cli queue client and get the stop channels
func (qu *QueueMg) AddConsumer(Name string, consumer func(protob.Message) error, dummyXMsg protob.Message, Config config.ConsumerConfig, cli queueClis.QueueClient) (string, []byte, error) {

	tXMsg := reflect.TypeOf(dummyXMsg).Elem()
	qid, qDef := ids.NewQueueID(Name, tXMsg)

	rqueue := qid                 // when the number of retys of a message is reach. We publish it in the first dead queue
	dlq := rqueue + "-dlq"        // first dead queue this is the queue we try to consume later if the configuration is enable
	dlq2 := rqueue + "-dlq2"      // second dead queue if a message in the first dead queue can not be consume we publish in the this
	uque := rqueue + "-Unmarshal" // if can not Unmarshal the message we publish it here

	fnUnmarshalAndConsume := func(qToRepublish string, DLQName string, UQName string, msg []byte, cli queueClis.QueueClient) {
		var wQueue mgs.QueueMsg
		errUnmarsalWrap := protob.Unmarshal(msg, &wQueue)

		if errUnmarsalWrap != nil {
			log.Printf("Could not Unmarsal WarpMsg %s", errUnmarsalWrap)
			cli.Publish(UQName, msg)
			return
		}

		if wQueue.Retrys >= int64(Config.Retry) {
			wQueue.Retrys = int64(0)
			bA, _ := wQueue.Marshal()
			cli.Publish(DLQName, bA)
			return
		}

		xMsg := reflect.New(tXMsg).Interface().(protob.Message)
		errUnmarsal := protob.Unmarshal(wQueue.GetMsg(), xMsg)

		if errUnmarsal != nil {
			log.Printf("Could not Unmarsal Msg %s", errUnmarsal)
			cli.Publish(UQName, msg)
			return
		}

		errOnConsume := consumer(xMsg)

		if errOnConsume != nil {
			wQueue.Retrys = wQueue.GetRetrys() + int64(1)
			wQueue.Exception = errOnConsume.Error()
			bA, _ := wQueue.Marshal()
			cli.Publish(qToRepublish, bA)

			log.Printf("Error on Consume: %s", errOnConsume)
			return
		}
	}

	CreateConsumerLoop := func(cLName string, startConsuming bool, tickerTime time.Duration, cli queueClis.QueueClient) (consumerLoop func(qToConsume string, DLQName string, UQName string), stopCh chan struct{}) {
		ticker := time.NewTicker(tickerTime)
		stopCh = make(chan struct{})
		consumerLoop = func(qToConsume string, DLQName string, UQName string) {
			consume := startConsuming
			for {
				select {
				case <-stopCh:
					ticker.Stop()
					return
				case <-ticker.C:
					consume = true
				default:
					if consume {
						if bytes, errorGetbytes := cli.Consume(qToConsume); errorGetbytes == nil && bytes != nil {
							fnUnmarshalAndConsume(qToConsume, DLQName, UQName, bytes, cli)
						} else {
							if errorGetbytes != nil {
								log.Printf("Error on get bytes: %s, in consumer loop: %s", errorGetbytes, cLName)
								consume = false
							}
							if !startConsuming {
								consume = false
							}
						}
					}
				}
			}
		}
		return
	}

	//Create queues
	errQid := cli.Make(rqueue)
	if errQid != nil {
		return "", []byte(""), errQid
	}
	errDlq := cli.Make(dlq)
	if errDlq != nil {
		return "", []byte(""), errDlq
	}
	errDlq2 := cli.Make(dlq2)
	if errDlq2 != nil {
		return "", []byte(""), errDlq2
	}
	errUnq := cli.Make(uque)
	if errUnq != nil {
		return "", []byte(""), errUnq
	}

	//Consume queues
	noConsumers := Config.NoGorutines
	if noConsumers > 1 {
		for i := 0; i < noConsumers; i++ {
			name := "regularConsumer_" + strconv.Itoa(i)
			regularConsumeri, stopChi := CreateConsumerLoop(name, true, 5*time.Second, cli)
			go regularConsumeri(rqueue, dlq, uque)
			qu.addStopChannels(qid, stopChi)
		}
	} else {
		regularConsumer, stopCh := CreateConsumerLoop("regularConsumer_1", true, 5*time.Second, cli)
		go regularConsumer(rqueue, dlq, uque)
		qu.addStopChannels(qid, stopCh)
	}

	if Config.ConsumeDL {
		consumerDLQLoop, stopCh := CreateConsumerLoop("dlqConsumer", false, Config.CheckDL, cli)
		go consumerDLQLoop(dlq, dlq2, uque)
		qu.addStopChannels(qid, stopCh)
	}
	return qid, []byte(qDef), nil
}

//StopConsume stop all gorutines what are consume the qid or qid-dlq
func (qu *QueueMg) StopConsume(qid string) {
	qu.mux.Lock()
	v, f := qu.stopConsumeCh[qid]
	if f {
		for _, i := range v {
			i <- struct{}{}
		}
	}
	delete(qu.stopConsumeCh, qid)
	qu.mux.Unlock()
}

func (qu *QueueMg) addStopChannels(qid string, stopCh ...chan struct{}) {
	qu.mux.Lock()
	v, f := qu.stopConsumeCh[qid]
	if !f {
		s := make([]chan struct{}, 0, 0)
		s = append(s, stopCh...)
		qu.stopConsumeCh[qid] = s
	} else {
		qu.stopConsumeCh[qid] = append(v, stopCh...)
	}
	qu.mux.Unlock()
}

//GetPublisher get a function to publish in the topic qid
func (qu *QueueMg) GetPublisher(qid string, balancer b.Balancer) func(x protob.Message) error { //add retry in publish?
	//Maybe we need to save this function?
	return func(x protob.Message) error {
		next, errBlancer := balancer.Next()

		if errBlancer != nil {
			return errBlancer
		}

		if cli := qu.PublisherCli.GetClient(next.GetMetadata()); cli != nil {

			bMsg, errMSG := protob.Marshal(x)
			if errMSG != nil {
				log.Printf("Error in Marshal msg %s", errMSG)
				return errMSG
			}

			newWrap := mgs.QueueMsg{
				Retrys: 0,
				Msg:    bMsg,
				//add UUID
			}

			bwrawp, errWrap := newWrap.Marshal()

			if errWrap != nil {
				log.Printf("Error in Marshal Wrap %s", errWrap)
				return errWrap
			}

			return cli.Publish(qid, bwrawp)
		}
		return errors.New("Queue Client not found for this: " + next.GetMetadata())
	}
}
