package Managers

import (
	"errors"
	"log"
	"reflect"
	"sync"
	"time"

	b "github.com/eddytrex/Mesh/Balancers"
	Callers "github.com/eddytrex/Mesh/Callers"
	Ids "github.com/eddytrex/Mesh/IDs"
	Installers "github.com/eddytrex/Mesh/Installers"
	mms "github.com/eddytrex/Mesh/MeshMsg"

	protobio "github.com/gogo/protobuf/io"
	protob "github.com/gogo/protobuf/proto"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

//FunctionMg
type FunctionMg struct {
	host     host.Host
	callers  map[string]func(address string, r protob.Message) (protob.Message, Callers.ErrorsAndMetrics)
	mcallers sync.Mutex
}

func NewFunctionMg(h host.Host) *FunctionMg {
	return &FunctionMg{
		host:     h,
		callers:  make(map[string]func(address string, r protob.Message) (protob.Message, Callers.ErrorsAndMetrics)),
		mcallers: sync.Mutex{},
	}
}

//InstallFunction install a function in the host
func (fnMG *FunctionMg) InstallFunction(Name string, fn func(protob.Message) (protob.Message, error), dummyXMsg protob.Message, dummyFnMsg protob.Message) (string, []byte) {
	tXMsg := reflect.TypeOf(dummyXMsg).Elem()
	tFnMsg := reflect.TypeOf(dummyFnMsg).Elem()
	FnID, vFnDefinition := Ids.NewFnID(Name, tXMsg, tFnMsg)

	WriteFnX := func(w protobio.WriteCloser, r protobio.ReadCloser, Result protob.Message) {
		if writeResultErr := w.WriteMsg(Result); writeResultErr != nil {
			log.Printf("Error in send Reuslt: %s", writeResultErr)
		}
	}

	ReadXAndApplyFuncion := func(w protobio.WriteCloser, r protobio.ReadCloser) mms.FnMSG {
		Metrics := make(map[string]float64)
		xMsg := reflect.New(tXMsg).Interface().(protob.Message)
		var result mms.FnMSG
		if unmarshallRequestError := r.ReadMsg(xMsg); unmarshallRequestError != nil {
			result = mms.NewFnMessage(tFnMsg, nil, unmarshallRequestError, Metrics)
		} else {
			rResult, fnError := fn(xMsg) //Apply function
			result = mms.NewFnMessage(tFnMsg, rResult, fnError, Metrics)
		}
		return result
	}

	Installers.LibP2P(protocol.ID(string(FnID)), fnMG.host, ReadXAndApplyFuncion, WriteFnX, false)

	return FnID, []byte(vFnDefinition)
}

//GetNewCaller create a new Caller with Id FnID
func (fnMG *FunctionMg) GetNewCaller(FnID string, tXMsg reflect.Type, tFnMsg reflect.Type) func(address string, r protob.Message) (protob.Message, Callers.ErrorsAndMetrics) {
	fnMG.mcallers.Lock()
	caller, found := fnMG.callers[string(FnID)]
	fnMG.mcallers.Unlock()
	if found {
		return caller
	}

	WriteX := func(w protobio.WriteCloser, x protob.Message) Callers.ErrorsAndMetrics {
		ListErrors := make([]error, 0, 0)
		result := Callers.ErrorsAndMetrics{Errors: ListErrors}
		if errorMarshallRequest := w.WriteMsg(x); errorMarshallRequest != nil {
			ListErrors = append(ListErrors, errorMarshallRequest)
			log.Printf("Error in send Reuslt: %s", errorMarshallRequest)
		}
		return result
	}

	ReadFnX := func(w protobio.WriteCloser, r protobio.ReadCloser) (result protob.Message, err Callers.ErrorsAndMetrics) {
		ListErrors := make([]error, 0, 0)
		var FnMsg mms.FnMSG
		if errorUnmarshallFnMsg := r.ReadMsg(&FnMsg); errorUnmarshallFnMsg != nil {
			ListErrors = append(ListErrors, errorUnmarshallFnMsg)
			return nil, Callers.ErrorsAndMetrics{Errors: ListErrors}
		}
		r.Close()
		if !FnMsg.IsNil {
			newFnMsg := reflect.New(tFnMsg).Interface().(protob.Message)
			if errUnmarshalValue := protob.Unmarshal(FnMsg.Value, newFnMsg); errUnmarshalValue != nil {
				ListErrors = append(ListErrors, errUnmarshalValue)
				return nil, Callers.ErrorsAndMetrics{Errors: ListErrors}
			}
			result = newFnMsg
		}

		if FnMsg.AnyError {
			ListErrors = append(ListErrors, errors.New(FnMsg.ErrorMessge))
		}
		return result, Callers.ErrorsAndMetrics{Errors: ListErrors, Metric: FnMsg.Metrics}
	}

	newCaller := func(address string, x protob.Message) (result protob.Message, err Callers.ErrorsAndMetrics) {
		return Callers.LibP2P(address, FnID, fnMG.host, WriteX, ReadFnX, x)
	}

	fnMG.mcallers.Lock()
	fnMG.callers[string(FnID)] = newCaller
	fnMG.mcallers.Unlock()
	return newCaller
}

//RemoteCall get the balancer and the caller and preform a call using the Next() peer and x argument
func RemoteCall(bl b.Balancer, fn func(address string, r protob.Message) (protob.Message, Callers.ErrorsAndMetrics)) func(x protob.Message) (protob.Message, error) {

	return func(x protob.Message) (protob.Message, error) {
		nextPeerToCall, ErrBalancer := bl.Next()

		if ErrBalancer != nil {
			return nil, ErrBalancer
		}

		startRequest := time.Now()
		FnX, errorAndMetrics := fn(nextPeerToCall.PeerID, x)
		requestTimeElapsed := time.Since(startRequest)

		//Local Metrics
		bl.RecordMetric(peer.ID(nextPeerToCall.PeerID), "TimeOfRequest", float64(requestTimeElapsed))
		bl.RecordMetric(peer.ID(nextPeerToCall.PeerID), "TotalRequest", 1)

		if errorAndMetrics.Errors != nil && len(errorAndMetrics.Errors) > 0 {
			bl.RecordMetric(peer.ID(nextPeerToCall.PeerID), "FiledRequest", 1)
			return nil, errorAndMetrics.Errors[0]
		}

		//Remote Metrics
		if errorAndMetrics.Metric != nil {
			for k, v := range errorAndMetrics.Metric {
				bl.RecordMetric(peer.ID(nextPeerToCall.PeerID), k, v)
			}
		}
		return FnX, nil
	}
}
