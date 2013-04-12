package otfc

import (
  "crypto/md5"
	"log"
)

const (
	INDEX_BLOCK_SIZE = 1023
)

// Represents an index record. The offset
type indexRecord struct {
	key    [16]byte //md5 hash of the config key
	offset uint32
	length uint32
}

type indexBlock struct {
	indices [INDEX_BLOCK_SIZE]indexRecord
}

func (rec indexRecord) print() {
	log.Printf("[%x : %x : %d]\n", rec.key, rec.offset, rec.length)
}

func (iBlock *indexBlock) print() {
	for _, x := range iBlock.indices {
		x.print()
	}
}

func (iBlock *indexBlock) set(key string, uint32 offset, uint32 length, int pos) (error err) {
  // md5(key)
  h := md5.New()
  h.Write([]byte(key))
  keyHash := h.Sum(nil)

  // Create new index record.
/*
  copyLen := copy(rec.key[:], keyHash)
  log.Println("Copy len: ", copyLen)
  rec.offset = offset
  rec.length = uint32(len(value))
  otfc.configPtr.index[count] = rec
*/
  iBlock[pos] = indexRecord{key: keyHash, offset: offset, legth: length}
}
