package PubSubClients

import (
	"context"

	flSub "github.com/libp2p/go-floodsub"
)

type FloodSubCli struct {
	cli *flSub.PubSub
	str string
}

func NewFloodSubCli(cli *flSub.PubSub) *FloodSubCli {
	return &FloodSubCli{
		cli: cli,
		str: string(flSub.ID),
	}
}

func (f *FloodSubCli) Subscribe(topic string) (*CliSubscription, error) {

	newS, erSub := f.cli.Subscribe(topic)
	if erSub != nil {
		return nil, erSub
	}
	ctx := context.Background()
	nextMsg := func() ([]byte, error) {

		cliMsg, err := newS.Next(ctx)

		if err != nil {
			return nil, err
		}
		return cliMsg.Data, nil
	}

	publisMsg := func(msg []byte) error {
		return f.cli.Publish(topic, msg)
	}

	unSubscribe := func() error {
		newS.Cancel()
		return nil
	}

	return &CliSubscription{
		NextMsg:     nextMsg,
		PublishMsg:  publisMsg,
		UnSubscribe: unSubscribe,
	}, nil
}

func (f *FloodSubCli) StrConnection() string {
	return f.str
}
