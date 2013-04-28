package otfc

import (
	"crypto/md5"
	"fmt"
	"log"
)

const (
	MAX_INDEX_RECORDS   = 1022
	INDEX_RECORD_SIZE   = 32
	INDEX_METADATA_SIZE = 32
)

const (
	INDEX_REC_STATUS_INACTIVE = 0x0
	INDEX_REC_STATUS_ACTIVE   = 0x1
	INDEX_REC_STATUS_DELETED  = 0x2
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
	//for i := uint32(0); i < iBlock.count; i++ {
	//iBlock.indices[i].print()
	//}
}

func (iBlock *indexBlock) set(
	key string,
	offset uint32,
	length uint32) (err error) {

	//log.Printf("Index size: %d\n", iBlock.count)
	if iBlock.count >= MAX_INDEX_RECORDS {
		return ConfigError{ERR_INDEX_FULL, fmt.Sprintf("key [%s]", key)}
	}
	// TODO: Check for key existance and overwrite.
	// Add the new index record at the end.
	rec := &(iBlock.indices[iBlock.count])

	keyHash := getKeyHash(key)
	rec.key = keyHash
	rec.dataOffset = offset
	rec.dataLength = length
	rec.status = INDEX_REC_STATUS_ACTIVE

	iBlock.count++
	return nil
}

func (iBlock *indexBlock) get(
	key string) (offset uint32, length uint32, err error) {
	indexRec, err := iBlock.find(key)
	if err != nil {
		return
	}
	if indexRec.status != INDEX_REC_STATUS_ACTIVE {
		err = ConfigError{
			ERR_INDEX_INACTIVE,
			fmt.Sprintf("key [%s], status[%d]", key, indexRec.status)}
		return
	}
	return indexRec.dataOffset, indexRec.dataLength, nil
}

func (iBlock *indexBlock) delete(key string) error {
	if iBlock.count == 0 {
		return nil
	}
	indexRecPtr, err := iBlock.find(key)
	if err != nil {
		return err
	}
	if indexRecPtr.status != INDEX_REC_STATUS_ACTIVE {
		indexRecPtr.status = INDEX_REC_STATUS_DELETED
		iBlock.count--
	}
	return nil
}

func (iBlock *indexBlock) find(key string) (*indexRecord, error) {
	inputKeyHash := getKeyHash(key)
	for i := uint32(0); i < iBlock.count; i++ {
		indexRecPtr := &(iBlock.indices[i])
		if indexRecPtr.key == inputKeyHash {
			return indexRecPtr, nil
		}
	}
	return nil, ConfigError{ERR_INDEX_KEY_NOT_FOUND, fmt.Sprintf("key [%s]", key)}
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
