package otfc

import (
	"fmt"
)

// TODO Implement a smarter management for the data block to avoid fragmentation.
const (
	DATA_BLOCK_SIZE = 256 * 1024 // 256k bytes
)

type dataBlock struct {
	data [DATA_BLOCK_SIZE]byte
}

// Copies data at the given offset
func (dBlock *dataBlock) set(
	dataOffset uint32,
	dataValue []byte) (newOffset uint32, err error) {

	bytesCopied := copy(dBlock.data[dataOffset:], dataValue)
	if bytesCopied != len(dataValue) {
		err = fmt.Errorf("Failed to copy data [%v] (%d bytes). Copied %d bytes",
			dataValue, len(dataValue), bytesCopied)
		return
	}
	newOffset = dataOffset + uint32(bytesCopied)
	return newOffset, nil
}

// Retrives 'length' bytes from the given offset.
func (dBlock *dataBlock) get(
	offset uint32,
	length uint32) (value []byte, err error) {

	if offset+length > DATA_BLOCK_SIZE {
		err = fmt.Errorf("Out of bound data access. Offset: [%d], length: [%d]",
			offset,
			length)
		return
	}
	value = dBlock.data[offset:(offset + length)]
	return value, nil
}
