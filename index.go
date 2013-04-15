package otfc

import (
	"crypto/md5"
	//"errors"
	"fmt"
	"log"
)

const (
	MAX_INDEX_RECORDS   = 1022
	INDEX_RECORD_SIZE   = 32
	INDEX_METADATA_SIZE = 32
)

type indexMetaData struct {
	count   uint32
	padding [28]byte
}

// Represents an index record. The offset
type indexRecord struct {
	key        [16]byte //md5 hash of the config key
	dataOffset uint32
	dataLength uint32
	status     byte
	padding    [7]byte
}

type indexBlock struct {
	indexMetaData
	indices [MAX_INDEX_RECORDS]indexRecord
}

func (rec indexRecord) print() {
	log.Printf("[%x : %x : %d]\n", rec.key, rec.dataOffset, rec.dataLength)
}

func (iBlock *indexBlock) print() {
	log.Printf("Total Index count: [%d]\n", iBlock.count)
	for i := uint32(0); i < iBlock.count; i++ {
		iBlock.indices[i].print()
	}
}

func (iBlock *indexBlock) set(
	key string,
	offset uint32,
	length uint32) (err error) {

	// TODO: Check for key existance and overwrite.
	// Add the new index record at the end.
	rec := &(iBlock.indices[iBlock.count])

	keyHash := getKeyHash(key)
	rec.key = keyHash
	rec.dataOffset = offset
	rec.dataLength = length

	iBlock.count++
	return nil
}

func (iBlock *indexBlock) get(
	key string) (offset uint32, length uint32, err error) {

	inputKeyHash := getKeyHash(key)
	for i := uint32(0); i < iBlock.count; i++ {
		indexRec := &(iBlock.indices[i])
		if indexRec.key == inputKeyHash {
			return indexRec.dataOffset, indexRec.dataLength, nil
		}
	}
	err = fmt.Errorf("Key [%s] not found", key)
	return 0, 0, err
}

func getKeyHash(key string) (ret [16]byte) {
	// md5 the key
	h := md5.New()
	h.Write([]byte(key))
	hash := h.Sum(nil)

	// TODO: Fix this copying. Make sure it copies only 16 bytes.
	copy(ret[:], hash)
	return ret
}
