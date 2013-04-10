package otfc

import (
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
