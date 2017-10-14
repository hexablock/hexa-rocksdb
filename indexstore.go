package hexarocksdb

import (
	"log"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
	"github.com/tecbot/gorocksdb"
)

// IndexStore implements an rocksdb KeylogIndex store interface
type IndexStore struct {
	opt *gorocksdb.Options
	db  *gorocksdb.DB

	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions

	// open indexes
	openIdxs *openIndexes
}

// NewIndexStore initializes an in-memory store for KeylogIndexes.
func NewIndexStore() *IndexStore {
	return &IndexStore{
		openIdxs: newOpenIndexes(),
		opt:      gorocksdb.NewDefaultOptions(),
		ro:       gorocksdb.NewDefaultReadOptions(),
		wo:       gorocksdb.NewDefaultWriteOptions(),
	}
}

// Open opens the index store for usage
func (store *IndexStore) Open(dir string) error {
	store.opt.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(store.opt, dir)
	if err == nil {
		store.db = db
	}
	return err
}

// Name returns the name of the index store
func (store *IndexStore) Name() string {
	return "rocksdb"
}

// NewKey creates a new KeylogIndex and adds it to the store.  It returns an error if it
// already exists
func (store *IndexStore) NewKey(key []byte) (hexalog.KeylogIndex, error) {
	if store.openIdxs.isOpen(key) {
		return nil, hexatype.ErrKeyExists
	}

	val, err := store.db.Get(store.ro, key)
	if err != nil {
		return nil, err
	}

	defer val.Free()
	if val.Data() != nil {
		return nil, hexatype.ErrKeyExists
	}

	kli := &KeylogIndex{
		db:  store.db,
		idx: hexalog.NewUnsafeKeylogIndex(key),
		wo:  gorocksdb.NewDefaultWriteOptions(),
		kh:  store.openIdxs,
	}
	store.openIdxs.register(kli)

	return kli, nil
}

// MarkKey sets the marker on a key.  If the key does not exist a new one is created.
// It returns the KeylogIndex or an error.
func (store *IndexStore) MarkKey(key, marker []byte) (hexalog.KeylogIndex, error) {
	if h, ok := store.openIdxs.get(key); ok {
		_, err := h.SetMarker(marker)
		return h.KeylogIndex, err
	}

	var (
		idx hexalog.KeylogIndex
		err error
	)

	idx, err = store.openIndex(key)
	if err == hexatype.ErrKeyNotFound {
		// Create a new key
		kli := &KeylogIndex{
			db:  store.db,
			idx: hexalog.NewUnsafeKeylogIndex(key),
			wo:  gorocksdb.NewDefaultWriteOptions(),
			kh:  store.openIdxs,
		}
		store.openIdxs.register(kli)
		idx = kli

	} else if err != nil {
		return nil, err
	}

	_, err = idx.SetMarker(marker)

	return idx, err
}

// GetKey returns a KeylogIndex from the store or an error if not found
func (store *IndexStore) GetKey(key []byte) (hexalog.KeylogIndex, error) {
	idx, ok := store.openIdxs.get(key)
	if ok {
		return idx.KeylogIndex, nil
	}

	return store.openIndex(key)
}

// RemoveKey removes the given key's index from the store.  It does NOT remove the associated
// entry hash id's
func (store *IndexStore) RemoveKey(key []byte) error {
	if store.openIdxs.isOpen(key) {
		return errIndexOpen
	}

	val, err := store.db.Get(store.ro, key)
	if err != nil {
		return err
	}

	defer val.Free()

	if val.Data() == nil {
		return hexatype.ErrKeyNotFound
	}

	return store.db.Delete(store.wo, key)
}

// Iter iterates over each key and index
func (store *IndexStore) Iter(cb func([]byte, hexalog.KeylogIndex) error) error {
	iter := store.db.NewIterator(store.ro)
	var err error

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		val := iter.Value().Data()

		var idx *KeylogIndex
		ih, ok := store.openIdxs.get(key)
		if !ok {
			if idx, err = store.makeKeylogIndex(val); err != nil {
				log.Println("[ERROR]", err)
				continue
			}
		} else {
			idx = ih.KeylogIndex
		}

		if err = cb(key, idx); err != nil {
			break
		}

	}

	iter.Close()

	return err
}

// Count returns the total number of keys in the index
func (store *IndexStore) Count() int64 {
	prop := store.db.GetProperty("rocksdb.estimate-num-keys")
	c, _ := strconv.ParseInt(prop, 10, 64)
	return c
}

// Close closes the index store by flushing all open indexes to rocka then
// closing rocks
func (store *IndexStore) Close() error {
	err := store.openIdxs.closeAll()
	store.db.Close()
	return err
}

func (store *IndexStore) openIndex(key []byte) (*KeylogIndex, error) {
	val, err := store.db.Get(store.ro, key)
	if err != nil {
		return nil, err
	}

	defer val.Free()

	data := val.Data()
	if data == nil {
		return nil, hexatype.ErrKeyNotFound
	}

	kli, err := store.makeKeylogIndex(data)
	if err == nil {
		store.openIdxs.register(kli)
	}

	return kli, err
}

// Unmarshal data to KeylogIndex
func (store *IndexStore) makeKeylogIndex(data []byte) (*KeylogIndex, error) {

	var ukli hexalog.UnsafeKeylogIndex
	if err := proto.Unmarshal(data, &ukli); err != nil {
		return nil, err
	}

	return &KeylogIndex{
		db:  store.db,
		idx: &ukli,
		wo:  gorocksdb.NewDefaultWriteOptions(),
		kh:  store.openIdxs,
	}, nil
}
