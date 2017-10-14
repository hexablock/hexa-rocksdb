package hexalog

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hexablock/hexatype"
)

func Test_RocksEntryStore(t *testing.T) {

	tmpdir, _ := ioutil.TempDir("/tmp", "hexalog-")
	defer os.RemoveAll(tmpdir)

	rdb := NewRocksEntryStore()
	if err := rdb.Open(tmpdir); err != nil {
		t.Fatal(err)
	}

	defer rdb.Close()

	ent := &Entry{
		Previous:  make([]byte, 32),
		Key:       []byte("key"),
		Timestamp: uint64(time.Now().UnixNano()),
	}

	id := ent.Hash((&hexatype.SHA256Hasher{}).New())
	if err := rdb.Set(id, ent); err != nil {
		t.Fatal(err)
	}

	ent1, err := rdb.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if string(ent1.Key) != "key" {
		t.Fatal("key mismatch")
	}

	id1 := ent1.Hash((&hexatype.SHA256Hasher{}).New())
	if bytes.Compare(id, id1) != 0 {
		t.Fatal("id mismatch")
	}

	if err = rdb.Delete(id1); err != nil {
		t.Fatal(err)
	}

	enterr, err := rdb.Get(id)
	if err == nil {
		t.Fatalf("should fail: '%+v'", enterr)
	} else {
		t.Log(err)
	}

}
