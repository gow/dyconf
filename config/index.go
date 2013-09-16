package config

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

type IndexKey [16]byte
type IndexKeySlice []IndexKey

// sort.Interface functions
func (keySlice IndexKeySlice) Len() int {
	return len(keySlice)
}
func (keySlice IndexKeySlice) Less(i, j int) bool {
	var keyA = keySlice[i]
	var keyB = keySlice[j]
	for index := 0; index < 16; index++ {
		if keyA[index] < keyB[index] {
			return true
		} else {
			return false
		}
	}
	return true
}
func (keySlice IndexKeySlice) Swap(i, j int) {
	keySlice[i], keySlice[j] = keySlice[j], keySlice[i]
}

// Represents an index record. The offset
type indexRecord struct {
	key        IndexKey //md5 hash of the config key
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
}

func (iBlock *indexBlock) set(
	key string,
	offset uint32,
	length uint32) (err error) {

	//log.Printf("Index size: %d\n", iBlock.count)
	if iBlock.count >= MAX_INDEX_RECORDS {
		return Error{ERR_INDEX_FULL, fmt.Sprintf("key [%s]", key)}
	}
	// TODO: Check for key existance and overwrite.
	// Add the new index record at the end.
	rec := &(iBlock.indices[iBlock.count])

	keyHash := getIndexKey(key)
	rec.key = keyHash
	rec.dataOffset = offset
	rec.dataLength = length
	rec.status = INDEX_REC_STATUS_ACTIVE

	iBlock.count++
	return nil
}

// Returns the offset & length of the data corresponding to the given Key (string)
func (iBlock *indexBlock) get(
	key string) (offset uint32, length uint32, err error) {
	indexKey := getIndexKey(key)
	return iBlock.getFromIndexKey(indexKey)
}

// Returns the offset & length of the data corresponding to the given IndexKey
func (iBlock *indexBlock) getFromIndexKey(
	key IndexKey) (offset uint32, length uint32, err error) {

	indexRec, err := iBlock.find(key)
	if err != nil {
		return
	}
	if indexRec.status != INDEX_REC_STATUS_ACTIVE {
		err = Error{
			ERR_INDEX_INACTIVE,
			fmt.Sprintf("key [%s], status[%d]", key, indexRec.status)}
		return
	}
	return indexRec.dataOffset, indexRec.dataLength, nil
}

// Returns all IndexKeys that are currently active.
func (iBlock *indexBlock) getAllIndexKeys() IndexKeySlice {
	var keys IndexKeySlice
	for i := uint32(0); i < iBlock.count; i++ {
		indexRecPtr := &(iBlock.indices[i])
		if indexRecPtr.status == INDEX_REC_STATUS_ACTIVE {
			keys = append(keys, indexRecPtr.key)
		}
	}
	return keys
}

func (iBlock *indexBlock) delete(key string) error {
	indexKey := getIndexKey(key)
	indexRecPtr, err := iBlock.find(indexKey)
	if err != nil {
		return err
	}
	// Copy the last element to the current position.
	*indexRecPtr = iBlock.indices[iBlock.count-1]
	// Reduce the index count by one now.
	iBlock.count--
	return nil
}

// Finds the indexRecord corresponding to the IndexKey
func (iBlock *indexBlock) find(key IndexKey) (*indexRecord, error) {
	for i := uint32(0); i < iBlock.count; i++ {
		indexRecPtr := &(iBlock.indices[i])
		if indexRecPtr.key == key {
			return indexRecPtr, nil
		}
	}
	return nil, Error{ERR_INDEX_KEY_NOT_FOUND, fmt.Sprintf("key [%s]", key)}
}

// Returns the IndexKey from key string
func getIndexKey(key string) (ret IndexKey) {
	// md5 the key
	h := md5.New()
	h.Write([]byte(key))
	hash := h.Sum(nil)

	// TODO: Fix this copying. Make sure it copies only 16 bytes.
	copy(ret[:], hash)
	return ret
}
