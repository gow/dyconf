package dyconf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

const (
	defaultIndexSize = 1 << 20 // 1 million
	sizeOfUint32     = 4
)

type dataPtr uint32

type index interface {
	get(key string) (dataPtr, error)
	set(key string, ptr dataPtr) error
}

type indexBlock struct {
	size uint32 // current size of the index.
	data []byte // Index data block
}

func (i *indexBlock) get(key string) (dataPtr, error) {
	h, err := defaultHashFunc(key)
	if err != nil {
		return 0, err
	}
	index := (h % i.size) * sizeOfUint32
	// These 4 bytes represent the pointer in data block.
	ptrBytes := i.data[index:(index + sizeOfUint32)]

	// Convert from bytes to data block pointer offset.
	var ptr dataPtr
	buf := bytes.NewReader(ptrBytes)
	err = binary.Read(buf, binary.LittleEndian, &ptr)
	if err != nil {
		return 0, fmt.Errorf("error in reading index: %s", err)
	}
	return ptr, nil
}

func (i *indexBlock) set(key string, ptr dataPtr) error {
	panic("Implement me!")
	return nil
}

type hashFunc func(key string) (uint32, error)

var defaultHashFunc = hashFuncFNV1a

var hashFuncFNV1a = func(key string) (uint32, error) {
	h := fnv.New32a() // Use FNV-1a hashing.
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
