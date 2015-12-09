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

type dataOffset uint32

type index interface {
	get(key string) (dataOffset, error)
	set(key string, offset dataOffset) error
}

type indexBlock struct {
	count uint32 // current size of the index.
	data  []byte // Index data block
}

func (i *indexBlock) get(key string) (dataOffset, error) {
	h, err := defaultHashFunc(key)
	if err != nil {
		return 0, err
	}
	index := (h % i.count) * sizeOfUint32
	// These 4 bytes represent the pointer in data block.
	ptrBytes := i.data[index:(index + sizeOfUint32)]

	// Convert from bytes to data block pointer offset.
	var offset dataOffset
	buf := bytes.NewReader(ptrBytes)
	err = binary.Read(buf, binary.LittleEndian, &offset)
	if err != nil {
		return 0, fmt.Errorf("error in reading index: %s", err)
	}
	return offset, nil
}

func (i *indexBlock) set(key string, offset dataOffset) error {
	// compute index offset.
	h, err := defaultHashFunc(key)
	if err != nil {
		return err
	}
	index := (h % i.count) * sizeOfUint32
	offsetBytes := i.data[index:(index + sizeOfUint32)]

	// Conver the offset into little-endian byte ordering.
	buf := &bytes.Buffer{}
	err = binary.Write(buf, binary.LittleEndian, offset)
	if err != nil {
		return err
	}

	// save at offsetBytes
	copy(offsetBytes, buf.Bytes())
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
