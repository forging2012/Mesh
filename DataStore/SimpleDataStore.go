package DataStore

import (
	dataStore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

type ServiceDataStore map[dataStore.Key]interface{}

func NewServieDataStore() ServiceDataStore {
	return ServiceDataStore(make(map[dataStore.Key]interface{}))
}

func (s ServiceDataStore) Put(key dataStore.Key, value interface{}) error {
	s[key] = value
	return nil
}

func (s ServiceDataStore) Get(key dataStore.Key) (interface{}, error) {
	val, found := s[key]
	if !found {
		return nil, dataStore.ErrNotFound
	}
	return val, nil
}

func (s ServiceDataStore) Has(key dataStore.Key) (exists bool, err error) {
	_, found := s[key]
	return found, nil
}

func (s ServiceDataStore) Delete(key dataStore.Key) (err error) {
	if _, found := s[key]; !found {
		return dataStore.ErrNotFound
	}
	delete(s, key)
	return nil
}

func (s ServiceDataStore) Query(q dsq.Query) (dsq.Results, error) {
	re := make([]dsq.Entry, 0, len(s))
	for k, v := range s {
		re = append(re, dsq.Entry{Key: k.String(), Value: v})
	}
	r := dsq.ResultsWithEntries(q, re)
	r = dsq.NaiveQueryApply(q, r)
	return r, nil
}

func (s ServiceDataStore) Batch() (dataStore.Batch, error) {
	return dataStore.NewBasicBatch(s), nil
}

func (s *ServiceDataStore) Close() error {
	return nil
}
