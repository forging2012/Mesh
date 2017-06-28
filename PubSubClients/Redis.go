package PubSubClients

import "github.com/go-redis/redis"

type RedisCli struct {
	cli           *redis.Client
	strConnection string
}

func NewRedisCli(str string) *RedisCli {
	rClient := redis.NewClient(&redis.Options{
		Addr:     str,
		Password: "",
		DB:       0,
	})
	return &RedisCli{
		cli:           rClient,
		strConnection: str,
	}
}
func (r *RedisCli) Subscribe(topic string) (*CliSubscription, error) {

	sub := r.cli.Subscribe(topic)
	c := sub.Channel()
	nextMsg := func() ([]byte, error) {
		va := <-c
		return []byte(va.Payload), nil
	}

	publishMsg := func(msg []byte) error {

		err := r.cli.Publish(topic, string(msg)).Err()
		if err != redis.Nil {
			return err
		}
		return nil
	}

	unSubscribe := func() error {
		err := sub.Unsubscribe(topic)
		if err != redis.Nil {
			return err
		}
		return nil
	}

	return &CliSubscription{
		NextMsg:     nextMsg,
		PublishMsg:  publishMsg,
		UnSubscribe: unSubscribe,
	}, nil
}

func (r *RedisCli) StrConnection() string {
	return r.strConnection
}
