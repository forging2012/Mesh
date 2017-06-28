package PubSubClients

import (
	ampq "github.com/streadway/amqp"
)

type RabbitMqCli struct {
	conn          *ampq.Connection
	channel       *ampq.Channel
	strConnection string
}

func NewRabbitMqCli(str string) *RabbitMqCli {
	conn, err := ampq.Dial(str)
	if err != nil {
		return nil
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil
	}
	return &RabbitMqCli{
		channel:       ch,
		conn:          conn,
		strConnection: str,
	}
}

func (rb *RabbitMqCli) Subscribe(topic string) (*CliSubscription, error) {

	err := rb.channel.ExchangeDeclare(topic, "topic", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	qT, err := rb.channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return nil, err
	}

	err = rb.channel.QueueBind(qT.Name, "#", topic, false, nil)
	if err != nil {
		return nil, err
	}

	msgC, err := rb.channel.Consume(qT.Name, "", true, false, false, false, nil)

	if err != nil {
		return nil, err
	}

	nextMsg := func() ([]byte, error) {
		value := <-msgC
		return value.Body, nil
	}

	publisMsg := func(msg []byte) error {
		err = rb.channel.Publish(topic, "", false, false, ampq.Publishing{ContentType: "text/plain", Body: []byte(msg)})
		return nil
	}

	unSubscribe := func() error {
		return rb.channel.QueueUnbind(qT.Name, "#", topic, nil)
	}

	return &CliSubscription{
		NextMsg:     nextMsg,
		PublishMsg:  publisMsg,
		UnSubscribe: unSubscribe,
	}, nil
}

func (rb *RabbitMqCli) StrConnection() string {
	return rb.strConnection
}
