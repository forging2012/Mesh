package PubSubClients

import (
	"sync"

	protob "github.com/gogo/protobuf/proto"
)

type Subscription struct {
	inChannles     []chan WrapMsg
	stopGourutines []chan struct{}
	m              *sync.Mutex

	marshalAndPublish  func(pubChannel chan protob.Message, stop chan struct{})
	getMsgAndUnmarshal func(fanOut func(msg WrapMsg), stop chan struct{})
	unSubscribeCli     func() error
}

func NewSubscription(marshalAndPublish func(pub chan protob.Message, stop chan struct{}), getMsgAndUnmarshal func(fanout func(msg WrapMsg), stop chan struct{}), unSubscribeCli func() error) *Subscription {
	return &Subscription{
		inChannles:         make([]chan WrapMsg, 0, 0),
		stopGourutines:     make([]chan struct{}, 0, 0),
		m:                  &sync.Mutex{},
		marshalAndPublish:  marshalAndPublish,
		getMsgAndUnmarshal: getMsgAndUnmarshal,
		unSubscribeCli:     unSubscribeCli,
	}
}

func (s *Subscription) GetPublisherChannel() chan<- protob.Message {
	Publish := make(chan protob.Message, 100)
	stop := s.newStopChn()
	go func() {
		s.marshalAndPublish(Publish, stop)
		close(Publish)
	}()
	return Publish
}

func (s *Subscription) GetNewMessagesChannel() (ResultNewMsgs <-chan WrapMsg) {
	if len(s.inChannles) > 0 {
		ResultNewMsgs = s.newChn(100)
		return
	}
	ResultNewMsgs = s.newChn(100)
	stop := s.newStopChn()

	go func() {
		s.getMsgAndUnmarshal(s.fanOut, stop)
	}()

	return ResultNewMsgs
}

func (s *Subscription) UnSubscribe() {
	s.m.Lock()
	error := s.unSubscribeCli()
	if error != nil {
		//log
	}
	for _, sc := range s.stopGourutines {
		sc <- struct{}{}
	}
	s.m.Unlock()
}

func (s *Subscription) newStopChn() (stop chan struct{}) {
	s.m.Lock()
	newChS := make(chan struct{})
	s.stopGourutines = append(s.stopGourutines, newChS)
	s.m.Unlock()
	return newChS
}

func (s *Subscription) newChn(buffered int) (msg chan WrapMsg) {
	s.m.Lock()
	newCh := make(chan WrapMsg, buffered)
	s.inChannles = append(s.inChannles, newCh)
	s.m.Unlock()
	return newCh
}

func (s *Subscription) fanOut(msg WrapMsg) {
	s.m.Lock()
	for _, c := range s.inChannles {
		c <- msg
	}
	s.m.Unlock()
}
