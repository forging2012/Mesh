package PubSubClients

type PubSubCli interface {
	Subscribe(topic string) (*CliSubscription, error)
	StrConnection() string
}

type CliSubscription struct {
	NextMsg     func() ([]byte, error)
	PublishMsg  func(msg []byte) error
	UnSubscribe func() error
}
