package IDs

import (
	"encoding/json"
	"reflect"

	jsonschema "github.com/alecthomas/jsonschema"
	mh "github.com/multiformats/go-multihash"
)

//NewFnID generate and id (FnID) and a description of a funccion(vDefintion) using the type of the Parameter and the type of the respose
func NewFnID(name string, tXMsg reflect.Type, tFnMsg reflect.Type) (string, string) {
	nameXMsg := tXMsg.Name()
	nameFnMsg := tFnMsg.Name()

	reflector := jsonschema.Reflector{}

	schemaXMsg := reflector.ReflectFromType(tXMsg)
	schemaFnMsg := reflector.ReflectFromType(tFnMsg)

	schemaXMI, _ := json.MarshalIndent(schemaXMsg, "", "	")
	schemaFNMI, _ := json.MarshalIndent(schemaFnMsg, "", "	")

	tDefinition := name + ":\n" + nameXMsg + ":" + string(schemaXMI) + "->" + nameFnMsg + ":" + string(schemaFNMI)

	FnID, _ := mh.Sum([]byte(tDefinition), mh.SHA2_256, -1)
	return "/fn/" + FnID.B58String(), tDefinition
}

//NewQueueID generate an ID and a QuDefinition using the type and a name
func NewQueueID(name string, dummyType reflect.Type) (string, string) {

	reflector := jsonschema.Reflector{}

	schemaType := reflector.Reflect(dummyType)
	schemaTypeMI, _ := json.MarshalIndent(schemaType, "", "	")

	QuDefinition := name + ":\n queue " + dummyType.Name() + ":" + string(schemaTypeMI)

	QuID, _ := mh.Sum([]byte(QuDefinition), mh.SHA2_256, -1)
	return "/qu/" + QuID.B58String(), QuDefinition
}

//NewPubSubID generate an ID and PubSubDefinition using the type and a name
func NewPubSubID(name string, dummyType reflect.Type) (string, string) {
	reflector := jsonschema.Reflector{}

	schemaType := reflector.Reflect(dummyType)
	schemaTypeMI, _ := json.MarshalIndent(schemaType, "", "	")

	PubSubDefinition := name + ":\n pubsubCh " + dummyType.Name() + ":" + string(schemaTypeMI)

	ChID, _ := mh.Sum([]byte(PubSubDefinition), mh.SHA2_256, -1)
	return "/ps/" + ChID.B58String(), PubSubDefinition
}
