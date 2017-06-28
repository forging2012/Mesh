package QueueClients

import (
	"github.com/go-redis/redis"
)

//Reis Queue Client using
type Redis struct {
	cli           *redis.Client
	strConnection string
}

//NewRedisClient create a new queue client using srv
func NewRedisClient(str string) *Redis {
	rClient := redis.NewClient(&redis.Options{
		Addr:     str,
		Password: "",
		DB:       0,
	})
	return &Redis{
		cli:           rClient,
		strConnection: str,
	}
}

//Make Create a queue in redis
func (r *Redis) Make(name string) error {
	return r.cli.Ping().Err()
}

//GetLenght get the lenght of a queue in redis
func (r *Redis) GetLenght(queue string) (int64, error) {
	v := r.cli.LLen(queue)
	if v.Err() != nil {
		return 0, v.Err()
	}
	return v.Val(), nil
}

//Consume consume the bytes in a queue if exist any
func (r *Redis) Consume(queue string) ([]byte, error) {
	llenqCmd := r.cli.LLen(queue)

	errLLen := llenqCmd.Err()
	if errLLen != nil {
		return nil, errLLen
	}

	if llenqCmd.Val() == 0 {
		return nil, nil
	}

	lPopMsg := r.cli.RPop(queue) //??
	bytes, lpopErr := lPopMsg.Bytes()

	if lpopErr != nil {
		if lpopErr == redis.Nil { // sigh
			return nil, nil
		}
		return nil, lpopErr
	}

	return bytes, nil
}

// GetStrConnection get the str to redis we use
func (r *Redis) GetStrConnection() string {
	return r.strConnection
}

//Publish Publisht the bytes in msg in the queue
func (r *Redis) Publish(queue string, msg []byte) error {
	cmd := r.cli.LPush(queue, msg)
	return cmd.Err()
}
