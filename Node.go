package Mesh

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	balancers "github.com/eddytrex/Mesh/Balancers"
	config "github.com/eddytrex/Mesh/Config"
	dataStore "github.com/eddytrex/Mesh/DataStore"
	fnMetadata "github.com/eddytrex/Mesh/FnMetadata"
	ids "github.com/eddytrex/Mesh/IDs"
	mng "github.com/eddytrex/Mesh/Managers"
	msg "github.com/eddytrex/Mesh/MeshMsg"
	PubSubClient "github.com/eddytrex/Mesh/PubSubClients"
	quClients "github.com/eddytrex/Mesh/QueueClients"

	protob "github.com/gogo/protobuf/proto"
	ipfsUtil "github.com/ipfs/go-ipfs-util"
	flSub "github.com/libp2p/go-floodsub"
	crypto "github.com/libp2p/go-libp2p-crypto"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	peer "github.com/libp2p/go-libp2p-peer"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	validations "github.com/libp2p/go-libp2p-record"
	swarm "github.com/libp2p/go-libp2p-swarm"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

//Node represent a collection of functions published in dht.
type Node struct {
	p2Pport string

	dht  *dht.IpfsDHT
	host host.Host

	metadaFn     *fnMetadata.FnMetadata
	fnMetadataID string

	blMng *mng.BalancerMg
	fnMng *mng.FunctionMg
	quMng *mng.QueueMg
	psMng *mng.PubSubMg

	flcli *flSub.PubSub
}

// create a 'Host' with a random peer to listen on the given address
func makeLibP2PBasicHost(port int) (host.Host, error) {
	listenaddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port) // Get all Addreses????

	addr, err := ma.NewMultiaddr(listenaddr)
	if err != nil {
		return nil, err
	}
	// /ip4/0.0.0.0/udp/%d/utp
	/*listenaddrUDP := fmt.Sprintf("/ip4/0.0.0.0/udp/%d/utp", port+1) // Get all Addreses
	addudp, err := ma.NewMultiaddr(listenaddrUDP)
	if err != nil {
		return nil, err
	}*/

	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	id, err := peer.IDFromPublicKey(pub)

	if err != nil {
		return nil, err
	}

	ps := pstore.NewPeerstore()
	ps.AddPrivKey(id, priv)
	ps.AddPubKey(id, pub)

	ctx := context.Background()

	networkSwarm, err := swarm.NewNetwork(ctx, []ma.Multiaddr{addr}, id, ps, nil)

	//utpT := utp.NewUtpTransport()
	//netw.Swarm().AddTransport(utpT)

	if err != nil {
		return nil, err
	}

	options := &bhost.HostOpts{
		NATManager: bhost.NewNATManager(networkSwarm),
	}
	log.Printf("I am %s/ipfs/%s\n", addr, id.Pretty())
	return bhost.NewHost(networkSwarm, options), nil
}

//NewNode Create a new node to offer funccions to the mesh
func NewNode(P2Pport int, pulicAddresses ...string) *Node {
	h, err := makeLibP2PBasicHost(P2Pport)

	if err != nil {
		log.Printf("%s", err)
		return nil
	}

	ctxDHT := context.Background()

	dstore := dataStore.NewServieDataStore()
	result := &Node{
		p2Pport: fmt.Sprintf(":%d", P2Pport),
		dht:     dht.NewDHT(ctxDHT, h, dstore),
		host:    h,
		fnMng:   mng.NewFunctionMg(h),
		quMng:   mng.NewQueuMg(quClients.NewPublisherClients()),
		psMng:   mng.NewPubSubMng(),
		flcli:   flSub.NewFloodSub(context.Background(), h),
	}

	for _, addr := range pulicAddresses {

		tptaddr := strings.Split(addr, "/ipfs/")

		multiAddress, errPublicAddress := ma.NewMultiaddr(tptaddr[0])
		pid := tptaddr[1]
		peerid, err := peer.IDB58Decode(pid)
		if err != nil {
			return nil
		}
		result.host.Peerstore().AddAddr(peerid, multiAddress, pstore.PermanentAddrTTL)
		result.dht.Update(ctxDHT, peerid)
		if errPublicAddress != nil {
			return nil
		}
	}

	result.addFnValidatorSelector()
	result.configureBalancer()

	return result
}

func (n *Node) addFnValidatorSelector() {
	createNewKeyValidator := func(prefix string) func(key string, val []byte) error {
		lprefix := len(prefix)
		return func(key string, val []byte) error {
			if len(key) < 5 {
				return errors.New("Invalid public key record key")
			}

			prefix := key[:lprefix]
			if prefix != prefix {
				return errors.New("The Key was not prefixed with " + prefix)
			}

			keyhash := []byte(key[lprefix:])
			if _, err := mh.Cast(keyhash); err != nil {
				return fmt.Errorf("The key did not contain valid multihash: %s", err)
			}

			pkh := ipfsUtil.Hash(val)
			if !bytes.Equal(keyhash, pkh) {
				return errors.New("The Public key does not match storage key")
			}
			return nil
		}

	}

	n.dht.Validator["fn"] = &validations.ValidChecker{
		Func: createNewKeyValidator("/fn/"),
		Sign: true,
	}
	n.dht.Selector["fn"] = func(key string, vals [][]byte) (int, error) {
		return 0, nil
	}

	n.dht.Validator["qu"] = &validations.ValidChecker{
		Func: createNewKeyValidator("/qu/"),
		Sign: true,
	}
	n.dht.Selector["qu"] = func(key string, vals [][]byte) (int, error) {
		return 0, nil
	}

	n.dht.Validator["ps"] = &validations.ValidChecker{
		Func: createNewKeyValidator("/ps/"),
		Sign: true,
	}
	n.dht.Selector["ps"] = func(key string, vals [][]byte) (int, error) {
		return 0, nil
	}

}

func getNewPeersFunction(n *Node) func(ID string) []peer.ID {
	getNewPeers := func(ID string) []peer.ID {
		ctxP := context.Background()
		ctx, cancel := context.WithTimeout(ctxP, time.Duration(5)*time.Minute)
		peers, _ := n.dht.GetValues(ctx, ID, 5)

		cancel()
		result := make([]peer.ID, 0, 0)
		if peers != nil {
			for _, v := range peers {
				result = append(result, v.From)
			}
			return result
		}
		return result
	}
	return getNewPeers
}

func getMetadataForKey(n *Node) func(qID string, newPeers []peer.ID) []*msg.FnMetadata {
	return func(qID string, newPeers []peer.ID) []*msg.FnMetadata {
		metadatResult := make([]*msg.FnMetadata, 0)
		XMeata := msg.XMetadata{FnID: qID}

		tXMsg := reflect.TypeOf(&msg.XMetadata{}).Elem()
		tFnMsg := reflect.TypeOf(&msg.FnMetadata{}).Elem()

		caller := n.fnMng.GetNewCaller(n.fnMetadataID, tXMsg, tFnMsg)

		for _, p := range newPeers {
			FnXMeatadata, err := caller(string(p), &XMeata)
			if (err.Errors != nil && len(err.Errors) == 0) && FnXMeatadata != nil {
				metadatResult = append(metadatResult, FnXMeatadata.(*msg.FnMetadata))
			}
			if err.Errors != nil && len(err.Errors) > 0 {
				log.Printf("Error on call Medata Fn: %s", err.Errors)
			}
		}
		return metadatResult
	}
}

func (n *Node) configureBalancer() {

	metadaFuncionName := "MetaDataFuncion"
	n.metadaFn = fnMetadata.NewFnMedata()

	//Warp of FnMetadata
	fnMetadaToInstall := func(X protob.Message) (protob.Message, error) {
		rx := X.(*msg.XMetadata)
		return (n.metadaFn).GetFnMetadata(rx.FnID)
	}

	fnMetadataID, _ := n.fnMng.InstallFunction(metadaFuncionName, fnMetadaToInstall, &msg.XMetadata{}, &msg.FnMetadata{})

	hostID := string(n.host.ID())
	n.metadaFn.AddFnMetadata(fnMetadataID, &msg.FnMetadata{PeerID: hostID, Metadata: hostID})
	n.fnMetadataID = fnMetadataID

	getMetadataForFn := func(FnId string, newPeers []peer.ID) []*msg.FnMetadata {
		metadatResult := make([]*msg.FnMetadata, 0)

		for _, p := range newPeers {
			metadatResult = append(metadatResult, &msg.FnMetadata{
				PeerID:   string(p),
				Metadata: string(p),
			})
		}
		return metadatResult
	}

	getNewPeers := getNewPeersFunction(n)

	n.blMng = mng.NewBalancerMg(getNewPeers, 2*time.Minute)
	n.blMng.AddFnGetMetada("/qu/", getMetadataForKey(n))
	n.blMng.AddFnGetMetada("/fn/", getMetadataForFn)

}

// GetHost return the libp2p host
func (n *Node) GetFlCli() *flSub.PubSub {
	return n.flcli
}

//BootstrapDHT Make a Bootstrap of the dht
func (n *Node) BootstrapDHT(dt time.Duration) error {
	ctxBootstrap := context.Background()
	bError := n.dht.Bootstrap(ctxBootstrap)
	timer := time.NewTimer(dt)
	<-timer.C
	return bError
}

//GetAddresses get all address in ipfs format .. I dont know if this going to work well
func (n *Node) GetAddresses() []string {
	addressWithID := make([]string, 0, 0)
	for _, add := range n.host.Addrs() {
		str := add.String() + "/ipfs/" + n.host.ID().Pretty()
		addressWithID = append(addressWithID, str)
	}
	return addressWithID
}

//PublishFunction register a funccion to be called in the mesh. The funcction is refferenced by the name and the type of the argument(dummyXMsg) and the type of the result(dummyFnMsg)
func (n *Node) PublishFunction(Name string, fn func(protob.Message) (protob.Message, error), dummyXMsg protob.Message, dummyFnMsg protob.Message) error {
	ctx := context.Background()

	FnID, vFnDefinition := n.fnMng.InstallFunction(Name, fn, dummyXMsg, dummyFnMsg)

	m := &msg.FnMetadata{
		PeerID:   n.host.ID().Pretty(),
		Metadata: n.host.ID().Pretty(),
	}
	n.metadaFn.AddFnMetadata(FnID, m)

	errorPut := n.dht.PutValue(ctx, string(FnID), vFnDefinition)
	if errorPut != nil {
		log.Printf("Error in puting the fn in the dht %s", errorPut)
	}
	return errorPut
}

// GetPublishedFunction return a function who represent a remote function Published in the mesh
func (n *Node) GetPublishedFunction(Name string, dummyXMsg, dummyFnMsg protob.Message) func(x protob.Message) (Fnx protob.Message, err error) {

	tXMsg := reflect.TypeOf(dummyXMsg).Elem()
	tFnMsg := reflect.TypeOf(dummyFnMsg).Elem()
	FnID, _ := ids.NewFnID(Name, tXMsg, tFnMsg)

	b := n.blMng.GetBalancer(FnID)

	if b == nil {
		b = balancers.NewBalancerRoundRobin()
		n.blMng.AddBalancer(FnID, b)
	}

	fn := n.fnMng.GetNewCaller(FnID, tXMsg, tFnMsg)

	wrapWithValidation := func(x protob.Message) (protob.Message, error) {
		switch reflect.TypeOf(x).Elem() {
		case tXMsg:
			return mng.RemoteCall(b, fn)(x)
		}
		return nil, errors.New("The argument is not of the type: " + tXMsg.Name())
	}

	return wrapWithValidation
}

//AddConsumer Register a fucction who consume messages from a topic created by a name and type of the argument(dummXMsg). Using the Queue Client given
func (n *Node) AddConsumer(Name string, consumer func(protob.Message) error, dummyXMsg protob.Message, Config config.ConsumerConfig, cli quClients.QueueClient) error {

	QuID, vQuDefinition, err := n.quMng.AddConsumer(Name, consumer, dummyXMsg, Config, cli)
	if err != nil {
		return err
	}

	metadata, err := quClients.GetFnMetadataByCli(cli, n.host.ID().Pretty())

	if err != nil {
		return err
	}
	n.metadaFn.AddFnMetadata(QuID, metadata)

	ctx := context.Background()
	erroPut := n.dht.PutValue(ctx, QuID, vQuDefinition)

	if erroPut != nil {
		log.Printf("Error in puting the Consumer in the dht %s", erroPut)
	}
	return erroPut
}

///Add a channel to StopConsume

//PublisherFunction get a function to publish in the topic with created by a name and type of the argument(dummXMsg). Using different different clients provided by the consumers
func (n *Node) PublisherFunction(Name string, dummyXMsg protob.Message) func(x protob.Message) error {
	tXMsg := reflect.TypeOf(dummyXMsg).Elem()
	QuID, _ := ids.NewQueueID(Name, tXMsg)

	b := n.blMng.GetBalancer(QuID)
	if b == nil {
		b = balancers.NewSimpleQueueBalancer(n.quMng.PublisherCli, QuID, 500*time.Millisecond) // ???
		n.blMng.AddBalancer(QuID, b)
	}

	wrapFnWithValidation := func(x protob.Message) error {
		switch reflect.TypeOf(x).Elem() {
		case tXMsg:
			return n.quMng.GetPublisher(QuID, b)(x)
		}
		return errors.New("The argument is not of the type: " + tXMsg.Name())
	}

	return wrapFnWithValidation
}

func (n *Node) searchAndMetadata(key string) (*msg.FnMetadata, error) {
	getNewPeers := getNewPeersFunction(n)

	peers := getNewPeers(key)

	if len(peers) <= 0 {
		return nil, errors.New("Key Not found " + key)
	}

	getMetadata := getMetadataForKey(n)
	metadatas := getMetadata(key, peers)

	if len(metadatas) <= 0 {
		return nil, errors.New("Key Not found " + key)
	}

	return metadatas[0], nil
}

//CreateTopic  other nodes can subscribe suing the cli. the node how create the topic is subscribe too
func (n *Node) CreateTopic(Name string, dummyXTopic protob.Message, cli PubSubClient.PubSubCli) (subscription *PubSubClient.Subscription, Error error) {
	tXTopci := reflect.TypeOf(dummyXTopic).Elem()
	ChID, TopicDef := ids.NewPubSubID(Name, tXTopci)

	//Search If exist and other topic with different cli using the metadata. if exist error
	metadata, _ := n.searchAndMetadata(ChID)

	if metadata != nil && metadata.Metadata != cli.StrConnection() {
		return nil, errors.New("The Topic exist using other client")
	}

	nMetadata, errC := PubSubClient.GetFnMetadataByCli(cli, n.host.ID().Pretty())
	if errC != nil {
		return nil, errC
	}

	sub, errSub := n.psMng.Subscribe(string(n.host.ID()), ChID, tXTopci, cli)

	if errSub != nil {
		return nil, errSub
	}

	ctx := context.Background()
	erroPut := n.dht.PutValue(ctx, ChID, []byte(TopicDef))

	if erroPut != nil {
		log.Printf("Error in puting the key %s in the dht %s", ChID, erroPut)
		return nil, erroPut
	}

	n.metadaFn.AddFnMetadata(ChID, nMetadata)

	return sub, nil
}

//JoinToTopic  using a Name and the Type of dummyXTopic return NeMessages is where you get the messages of other publishers and Publish a channel you can publish your own
func (n *Node) JoinToTopic(Name string, dummyXTopic protob.Message) (subscription *PubSubClient.Subscription, Error error) {
	tXTopci := reflect.TypeOf(dummyXTopic).Elem()
	ChID, _ := ids.NewPubSubID(Name, tXTopci)

	metadata, errorMetadata := n.searchAndMetadata(ChID)

	if errorMetadata != nil {
		return nil, errorMetadata
	}

	cli, errCli := PubSubClient.GetClitByMetadata(metadata, n.flcli)
	if errCli != nil {
		return nil, errCli
	}

	sub, errSub := n.psMng.Subscribe(string(n.host.ID()), ChID, tXTopci, cli)

	if errSub != nil {
		return nil, errSub
	}
	return sub, nil
}
