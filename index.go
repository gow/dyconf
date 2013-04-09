package otfc

import (
	"log"
)

// Represents an index record. The offset
type indexRecord struct {
	key    [16]byte //md5 hash of the config key
	offset uint32
	length uint32
}

func (rec indexRecord) print() {
	log.Printf("[%x : %x : %d]\n", rec.key, rec.offset, rec.length)
}
