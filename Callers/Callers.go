package Callers

import (
	"context"
	"io"
	"net/http"

	ggio "github.com/gogo/protobuf/io"
	protobio "github.com/gogo/protobuf/io"
	protob "github.com/gogo/protobuf/proto"
	"github.com/jbenet/go-context/io"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

type ErrorsAndMetrics struct {
	Errors []error
	Metric map[string]float64
}

func newReaderAndWriter(ctx context.Context, sw io.Writer, sr io.Reader) (w protobio.WriteCloser, r protobio.ReadCloser) {
	cr := ctxio.NewReader(ctx, sr)
	cw := ctxio.NewWriter(ctx, sw)
	r = ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w = ggio.NewDelimitedWriter(cw)
	return w, r
}

// LibP2P preforms a call to Pp2
func LibP2P(address string, FnID string, host host.Host, WriteX func(w protobio.WriteCloser, x protob.Message) ErrorsAndMetrics, ReadFnX func(w protobio.WriteCloser, r protobio.ReadCloser) (result protob.Message, err ErrorsAndMetrics), x protob.Message) (result protob.Message, err ErrorsAndMetrics) {
	ListErrors := make([]error, 0, 0)

	ctx := context.Background()
	pathString := string(FnID)

	stream, errorNewStream := host.NewStream(ctx, peer.ID(address), protocol.ID(pathString))
	if errorNewStream != nil {
		ListErrors = append(ListErrors, errorNewStream)
		return nil, ErrorsAndMetrics{Errors: ListErrors}
	}

	w, r := newReaderAndWriter(ctx, stream, stream)
	marshallerror := WriteX(w, x)
	if len(marshallerror.Errors) > 0 {
		return nil, marshallerror
	}
	return ReadFnX(w, r)
}

//HTTP preform a call throug http
func HTTP(address string, FnID string, WriteX func(w protobio.WriteCloser, x protob.Message) ErrorsAndMetrics, ReadFnX func(w protobio.WriteCloser, r protobio.ReadCloser) (result protob.Message, err ErrorsAndMetrics), x protob.Message) (result protob.Message, err ErrorsAndMetrics) {
	ListErrors := make([]error, 0, 0)

	ctx := context.Background()
	pathString := string(FnID)

	rRequest, wRequest := io.Pipe()
	w, _ := newReaderAndWriter(ctx, wRequest, nil)

	go func() {
		WriteX(w, x)
		wRequest.Close()
	}()

	cli := http.Client{}
	respose, httperr := cli.Post(address+pathString, "", rRequest)

	if httperr != nil {
		ListErrors = append(ListErrors, httperr)
		return nil, ErrorsAndMetrics{Errors: ListErrors}
	}

	reader := respose.Body
	cr := ctxio.NewReader(ctx, reader)
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax)

	return ReadFnX(w, r)
}
