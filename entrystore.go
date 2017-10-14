package hexarocksdb

import (
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
	"github.com/tecbot/gorocksdb"
)

// EntryStore is an entry store using rocksdb as the backend
type EntryStore struct {
	opt *gorocksdb.Options
	db  *gorocksdb.DB

	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}

// NewEntryStore inits a new rocksdb backed entry store with defaults
func NewEntryStore() *EntryStore {
	return &EntryStore{
		opt: gorocksdb.NewDefaultOptions(),
		ro:  gorocksdb.NewDefaultReadOptions(),
		wo:  gorocksdb.NewDefaultWriteOptions(),
	}
}

// Name returns the name of the rocksdb entry store
func (store *EntryStore) Name() string {
	return "rocksdb"
}

// Open opens the rocks store for writing
func (store *EntryStore) Open(datadir string) error {
	store.opt.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(store.opt, datadir)
	if err == nil {
		store.db = db
	}
	return err
}

// Get gets an entry by the id
func (store *EntryStore) Get(id []byte) (*hexalog.Entry, error) {
	sl, err := store.db.Get(store.ro, id)
	if err != nil {
		return nil, err
	}
	defer sl.Free()

	data := sl.Data()
	if data == nil {
		return nil, hexatype.ErrEntryNotFound
	}

	var entry hexalog.Entry
	err = proto.Unmarshal(data, &entry)
	return &entry, err
}

// Set sets the entry to the store by the id
func (store *EntryStore) Set(id []byte, entry *hexalog.Entry) error {
	val, err := proto.Marshal(entry)
	if err == nil {
		err = store.db.Put(store.wo, id, val)
	}
	return err
}

// Delete deletes an entry by the id
func (store *EntryStore) Delete(id []byte) error {
	return store.db.Delete(store.wo, id)
}

// Count returns the approximate entry count
func (store *EntryStore) Count() int64 {
	prop := store.db.GetProperty("rocksdb.estimate-num-keys")
	c, _ := strconv.ParseInt(prop, 10, 64)
	return c
}

// Close closes the rocks store after which it can no longer be used.
func (store *EntryStore) Close() error {
	store.db.Close()
	return nil
}
