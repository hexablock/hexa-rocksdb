

CGO_CFLAGS = -I$(shell pwd)/rocksdb/include
#CGO_LDFLAGS = -L$(shell pwd)/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
CGO_LDFLAGS = -L$(shell pwd)/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2

# Build rocksdb static lib
dep-rocks:
	cd rocksdb && make static_lib

# Get and build rocksdb for go after a static lib has been generated
dep-gorocks:
	CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go get -u -v github.com/tecbot/gorocksdb

test:
	go test -v -cover .

clean:
	rm -rf ./tmp/*
