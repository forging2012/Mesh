package Installers

import (
	"context"
	"io"
	"net/http"

	msgs "github.com/eddytrex/Mesh/MeshMsg"
	ggio "github.com/gogo/protobuf/io"
	protobio "github.com/gogo/protobuf/io"
	protob "github.com/gogo/protobuf/proto"
	"github.com/jbenet/go-context/io"
	host "github.com/libp2p/go-libp2p-host"
	inet "github.com/libp2p/go-libp2p-net"
	net "github.com/libp2p/go-libp2p-net"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

func newReaderAndWriter(ctx context.Context, sw io.Writer, sr io.Reader) (w protobio.WriteCloser, r protobio.ReadCloser) {
	cr := ctxio.NewReader(ctx, sr)
	cw := ctxio.NewWriter(ctx, sw)
	r = ggio.NewDelimitedReader(cr, inet.MessageSizeMax)
	w = ggio.NewDelimitedWriter(cw)
	return w, r
}

func LibP2P(p protocol.ID, host host.Host, ReadXAndApplyFuncion func(w protobio.WriteCloser, r protobio.ReadCloser) msgs.FnMSG, WriteFnX func(w protobio.WriteCloser, r protobio.ReadCloser, Result protob.Message), continuos bool) {
	ctx := context.Background()

	handlerLipP2P := func(s net.Stream) {
		w, r := newReaderAndWriter(ctx, s, s)
		result := ReadXAndApplyFuncion(w, r)
		WriteFnX(w, r, &result)
		if continuos { //if is this handler is called continuosly, create a loop to not close the stream ?
			for {
				result := ReadXAndApplyFuncion(w, r)
				WriteFnX(w, r, &result)
			}
		}
	}
	host.SetStreamHandler(p, handlerLipP2P) // new host handler
}

func HTTP(path string, s *http.ServeMux, ReadXAndApplyFuncion func(w protobio.WriteCloser, r protobio.ReadCloser) msgs.FnMSG, WriteFnX func(w protobio.WriteCloser, r protobio.ReadCloser, Result protob.Message)) {
	ctx := context.Background()
	httpHandlerService := func(wr http.ResponseWriter, req *http.Request) {
		sReader := req.Body
		w, r := newReaderAndWriter(ctx, wr, sReader)
		result := ReadXAndApplyFuncion(w, r)
		WriteFnX(w, r, &result)
	}
	s.HandleFunc(path, httpHandlerService)
}
