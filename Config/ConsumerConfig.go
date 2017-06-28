package Config

import "time"

//ConsumerConfig  contain the configuration of how many times  will try to consme a msg from the original queue and the interval Between consume dlq
type ConsumerConfig struct {
	Retry       int           // Retrys before send it to dlq queue
	ConsumeDL   bool          // Try to consume from the dlq queue
	CheckDL     time.Duration // Interval Time to checks in the dlq
	NoGorutines int           // Number of gorutines consumming from the queue
}
