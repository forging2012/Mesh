package PubSubClients

import(
		protob "github.com/gogo/protobuf/proto"

)

type WrapMsg struct {
	ID  string
	Msg protob.Message
}
