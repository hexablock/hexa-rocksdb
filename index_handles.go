package hexarocksdb

import (
	"errors"
	"sync"

	"github.com/hexablock/hexatype"
)

var (
	errIndexOpen = errors.New("KeylogIndex is open")
)

type indexHandle struct {
	cnt int
	*KeylogIndex
}

type openIndexes struct {
	mu sync.RWMutex
	m  map[string]*indexHandle
}

func (oi *openIndexes) closeAll() error {
	var err error
	oi.mu.Lock()
	for _, v := range oi.m {
		if er := v.Flush(); er != nil {
			err = er
		}
	}
	oi.m = nil
	oi.mu.Unlock()
	return err
}

func (oi *openIndexes) close(key []byte) error {
	k := string(key)

	oi.mu.Lock()
	defer oi.mu.Unlock()

	if val, ok := oi.m[k]; ok {
		val.cnt--
		if val.cnt == 0 {
			val.Flush()
			delete(oi.m, k)
		}
		return nil
	}
	return hexatype.ErrKeyNotFound
}

// get an open index and up the ref count
func (oi *openIndexes) get(key []byte) (*indexHandle, bool) {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	ih, ok := oi.m[string(key)]
	if ok {
		ih.cnt++
	}
	return ih, ok
}

func (oi *openIndexes) isOpen(key []byte) bool {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	_, ok := oi.m[string(key)]
	return ok
}

func (oi *openIndexes) register(kli *KeylogIndex) {
	oi.mu.Lock()
	oi.m[string(kli.Key())] = &indexHandle{cnt: 1, KeylogIndex: kli}
	oi.mu.Unlock()
}

func newOpenIndexes() *openIndexes {
	return &openIndexes{m: make(map[string]*indexHandle)}
}
