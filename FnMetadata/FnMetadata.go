package FnMetadata

import (
	"errors"
	"sync"

	msg "github.com/eddytrex/Mesh/MeshMsg"
)

var errNoMetadata = errors.New("Metadata do not exist")

//FnMetadata record the metadata(like string connection if is necessary ) of the functions  and consumers and other objects published in the dht
type FnMetadata struct {
	InstalledFunctions map[string]*msg.FnMetadata
	Mx                 *sync.Mutex
}

//NewFnMedata Create a new Meatada function
func NewFnMedata() *FnMetadata {
	return &FnMetadata{
		InstalledFunctions: make(map[string]*msg.FnMetadata),
		Mx:                 &sync.Mutex{},
	}
}

//AddFnMetadata  Save new Register FnMetadata with a specific Id
func (s *FnMetadata) AddFnMetadata(ID string, m *msg.FnMetadata) {
	s.Mx.Lock()
	s.InstalledFunctions[ID] = m
	s.Mx.Unlock()
}

//GetFnMetadata get the Metdata of the ID or error if do not exist
func (s *FnMetadata) GetFnMetadata(ID string) (*msg.FnMetadata, error) {
	s.Mx.Lock()
	defer s.Mx.Unlock()
	v, ok := s.InstalledFunctions[ID]
	if !ok {
		return nil, errNoMetadata
	}

	return v, nil
}
