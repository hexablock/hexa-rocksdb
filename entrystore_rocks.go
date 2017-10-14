package hexalog

import (
	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexatype"
	"github.com/tecbot/gorocksdb"
)

// RocksEntryStore is an entry store using rocksdb as the backend
type RocksEntryStore struct {
	opt *gorocksdb.Options
	db  *gorocksdb.DB

	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

// NewRocksEntryStore inits a new rocksdb backed entry store with defaults
func NewRocksEntryStore() *RocksEntryStore {
	return &RocksEntryStore{
		opt: gorocksdb.NewDefaultOptions(),
		ro:  gorocksdb.NewDefaultReadOptions(),
		wo:  gorocksdb.NewDefaultWriteOptions(),
	}
}

// Name returns the name of the rocksdb entry store
func (store *RocksEntryStore) Name() string {
	return "rocksdb"
}

// Get gets an entry by the id
func (store *RocksEntryStore) Get(id []byte) (*Entry, error) {
	sl, err := store.db.Get(store.ro, id)
	if err != nil {
		return nil, err
	}
	data := sl.Data()
	if data == nil || len(data) == 0 {
		return nil, hexatype.ErrEntryNotFound
	}
	defer sl.Free()

	var entry Entry
	err = proto.Unmarshal(data, &entry)
	return &entry, err
}

// Open opens the rocks store for writing
func (store *RocksEntryStore) Open(datadir string) error {
	store.opt.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(store.opt, datadir)
	if err == nil {
		store.db = db
	}
	return err
}

// Set sets the entry to the store by the id
func (store *RocksEntryStore) Set(id []byte, entry *Entry) error {
	val, err := proto.Marshal(entry)
	if err == nil {
		err = store.db.Put(store.wo, id, val)
	}
	return err
}

// Delete deletes an entry by the id
func (store *RocksEntryStore) Delete(id []byte) error {
	return store.db.Delete(store.wo, id)
}

// Count always returns -1 as rocks does not have a way to get the exact key count
func (store *RocksEntryStore) Count() int {
	return -1
}

// Close closes the rocks store after which it can no longer be used.
func (store *RocksEntryStore) Close() error {
	store.db.Close()
	return nil
}
