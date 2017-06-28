package MeshMsg

import (
	"fmt"
	"reflect"

	protob "github.com/gogo/protobuf/proto"
	ma "github.com/multiformats/go-multiaddr"
)

//NewFnMessage generate a message to be written
func NewFnMessage(outType reflect.Type, value protob.Message, outError error, metrics map[string]float64) FnMSG {
	isNil := value == nil
	anyError := outError != nil
	errorMessge := fmt.Sprintf("%s", outError)

	if isNil {
		value = reflect.New(outType).Interface().(protob.Message)
	}

	valueBytes, _ := protob.Marshal(value)

	return FnMSG{
		AnyError:    anyError,
		ErrorMessge: errorMessge,
		IsNil:       isNil,
		Metrics:     metrics,
		Value:       valueBytes,
	}
}

//NewXMessage generate a new XMSG
func NewXMessage(xType reflect.Type, xValue protob.Message) XMSG {
	isNil := xValue == nil
	if isNil {
		xValue = reflect.New(xType).Interface().(protob.Message)
	}
	xValueBytes, _ := protob.Marshal(xValue)
	return XMSG{
		IsNil: isNil,
		Value: xValueBytes,
	}
}

//GetAddress get the first address
func (fnx *FnMetadata) GetAddress(peerAddres []ma.Multiaddr) string {

	return peerAddres[0].String()
}
