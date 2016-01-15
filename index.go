package dyconf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

const (
	sizeOfUint32 = 4
	sizeOfUint16 = 2
)

type dataOffset uint32

type index interface {
	get(key string) (dataOffset, error)
	set(key string, offset dataOffset) error
}

type indexBlock struct {
	size uint32 // current size of the index.
	data []byte // Index data block
}

// offsets returns all the valid datablock offsets set in the index.
func (i *indexBlock) getAll() ([]dataOffset, error) {
	var ret []dataOffset
	for idx := uint32(0); idx < i.size; idx++ {
		offset, err := i.offset(idx)
		if err != nil {
			return nil, err
		}
		if offset != 0 {
			ret = append(ret, offset)
		}
	}
	return ret, nil
}

func (i *indexBlock) offset(idx uint32) (dataOffset, error) {
	// These 4 bytes represent the pointer in data block.
	ptrBytes := i.data[idx : idx+sizeOfUint32]

	// Convert from bytes to data block pointer offset.
	var offset dataOffset
	buf := bytes.NewReader(ptrBytes)
	err := binary.Read(buf, binary.LittleEndian, &offset)
	if err != nil {
		return 0, fmt.Errorf("error in reading index: %s", err)
	}
	return offset, nil
}

func (i *indexBlock) get(key string) (dataOffset, error) {
	h, err := defaultHashFunc(key)
	if err != nil {
		return 0, err
	}
	index := (h % i.size) * sizeOfUint32
	return i.offset(index)
}

func (i *indexBlock) set(key string, offset dataOffset) error {
	// compute index offset.
	h, err := defaultHashFunc(key)
	if err != nil {
		return err
	}
	index := (h % i.size) * sizeOfUint32
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
