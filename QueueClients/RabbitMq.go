package QueueClients

import (
	"log"
	"sync"

	ampq "github.com/streadway/amqp"
)

//RabbitMq Represent a queue client using RabbitMq
type RabbitMq struct {
	StrConnection  string
	Conn           *ampq.Connection
	Ch             *ampq.Channel
	ChannelByQueue map[string]<-chan ampq.Delivery
	muxCh          *sync.Mutex
}

//NewRabbitMq create a new queue client using RabbitMq
func NewRabbitMq(strConnection string) *RabbitMq {
	conn, err := ampq.Dial(strConnection)

	if err != nil {
		log.Printf("Error on Dial ampq: %s", err)
		return nil
	}

	ch, errChn := conn.Channel()
	if errChn != nil {
		log.Printf("Error on get the channel ampq %s", errChn)
		return nil
	}

	return &RabbitMq{
		Conn:           conn,
		StrConnection:  strConnection,
		Ch:             ch,
		ChannelByQueue: make(map[string]<-chan ampq.Delivery),
		muxCh:          &sync.Mutex{},
	}
}

//GetStrConnection return the String connection used to create the client
func (r *RabbitMq) GetStrConnection() string {
	return r.StrConnection
}

//Make create a simple queue in rabitmq
func (r *RabbitMq) Make(queue string) error {
	_, errC := r.Ch.QueueDeclare(queue, true, true, false, false, nil)
	mgs, errConsume := r.Ch.Consume(queue, "", true, false, false, false, nil)

	if errConsume != nil {
		return errConsume
	}
	r.muxCh.Lock()
	r.ChannelByQueue[queue] = mgs
	r.muxCh.Unlock()
	return errC
}

//Publish publish the msg bytes in the queue using rabbitmq
func (r *RabbitMq) Publish(queue string, msg []byte) error {
	return r.Ch.Publish("", queue, false, false, ampq.Publishing{
		ContentType: "text/plain",
		Body:        msg,
	})

}

//GetLenght get the length  of a queue in rabbitmq
func (r *RabbitMq) GetLenght(queue string) (int64, error) {
	q, err := r.Ch.QueueInspect(queue)
	if err != nil {
		return 0, err
	}
	return int64(q.Messages), nil
}

//Consume consume the bytes in a queue in rabbitmq
func (r *RabbitMq) Consume(queue string) ([]byte, error) {
	r.muxCh.Lock()
	ch, exist := r.ChannelByQueue[queue]
	r.muxCh.Unlock()

	if !exist {
		ch, errConsume := r.Ch.Consume(queue, "", true, false, false, false, nil)
		r.muxCh.Lock()
		r.ChannelByQueue[queue] = ch
		r.muxCh.Unlock()

		if errConsume != nil {
			return nil, errConsume
		}
	}

	select {
	case bMsg := <-ch:
		return bMsg.Body, nil
	default:
		return nil, nil
	}
}
