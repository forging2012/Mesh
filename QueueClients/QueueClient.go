package QueueClients

//QueueClient is a Collection of metdos used to consume and publish in a queue server
type QueueClient interface {
	Make(name string) error
	Consume(queue string) ([]byte, error)
	Publish(queue string, msg []byte) error
	GetStrConnection() string
	GetLenght(queue string) (int64, error)
}
