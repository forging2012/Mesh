package PubSubClients

import (
	"errors"
	"sync"
)

type PubSubClients struct {
	clients map[string]PubSubCli //Clients by strConnection
	m       *sync.Mutex
}

func NewPubSubClients() PubSubClients {
	return PubSubClients{
		clients: make(map[string]PubSubCli),
		m:       &sync.Mutex{},
	}
}

func (p *PubSubClients) AddClient(cli PubSubCli) {
	str := cli.StrConnection()
	p.m.Lock()
	defer p.m.Unlock()

	_, exist := p.clients[str]
	if !exist {
		p.clients[str] = cli
	}
}

func (p *PubSubClients) Get(str string) (PubSubCli, error) {
	p.m.Lock()
	defer p.m.Unlock()

	cli, exist := p.clients[str]
	if !exist {
		return nil, errors.New("Client Do not exist")
	}
	return cli, nil
}
