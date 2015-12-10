package dyconf

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/facebookgo/stackerr"
)

type dataStore interface {
	save(key string, data []byte) (dataOffset, error)
	update(offset dataOffset, key string, data []byte) error
	fetch(offset dataOffset, key string) ([]byte, error)
}

type dataBlock struct {
	//maxDataBlockOffset dataOffset
	writeOffset dataOffset
	block       []byte
}

type record interface {
	read([]byte) error
	write([]byte)
	size() uint32
}

type dataRecord struct {
	totalSize uint32
	keySize   uint16
	dataSize  uint32
	key       []byte
	data      []byte
	next      dataOffset
}

func (r *dataRecord) read(block []byte) error {
	buf := bytes.NewReader(block)

	// Read Total size.
	err := binary.Read(buf, binary.LittleEndian, &r.totalSize)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// read key size.
	err = binary.Read(buf, binary.LittleEndian, &r.keySize)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// read data size
	err = binary.Read(buf, binary.LittleEndian, &r.dataSize)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// allocate key and then read into it.
	r.key = make([]byte, r.keySize)
	err = binary.Read(buf, binary.LittleEndian, &r.key)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// allocate data and then read into it.
	r.data = make([]byte, r.dataSize)
	err = binary.Read(buf, binary.LittleEndian, &r.data)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// Finally read the next pointer.
	err = binary.Read(buf, binary.LittleEndian, &r.next)
	if err != nil {
		return stackerr.Wrap(err)
	}

	return nil
}

func (r *dataRecord) bytes() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, r.size())
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	err = binary.Write(buf, binary.LittleEndian, r.keySize)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	err = binary.Write(buf, binary.LittleEndian, r.dataSize)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	err = binary.Write(buf, binary.LittleEndian, r.key)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	err = binary.Write(buf, binary.LittleEndian, r.data)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	err = binary.Write(buf, binary.LittleEndian, r.next)
	if err != nil {
		return nil, stackerr.Wrap(err)
	}

	return buf.Bytes(), nil
}

func (r *dataRecord) size() uint32 {
	if r.totalSize != 0 {
		return r.totalSize
	}
	size := uint32(0)
	size += sizeOfUint32        // totalSize field
	size += sizeOfUint16        // keySize field
	size += sizeOfUint32        // dataSize field
	size += uint32(len(r.key))  // key field
	size += uint32(len(r.data)) // data field
	size += sizeOfUint32        // next field
	fmt.Println("Size is ", size)
	return size
}

func (r *dataRecord) write(block []byte) error {
	bytesToWrite, err := r.bytes()
	if err != nil {
		return stackerr.Wrap(err)
	}

	if len(block) < len(bytesToWrite) {
		return stackerr.Newf(
			"insufficient space. Unable to write the key [%s]. Total space needed: [%d]",
			string(r.key),
			r.totalSize,
		)
	}

	// Write the serialized data into the block.
	writtenCount := copy(block, bytesToWrite)
	if writtenCount != len(bytesToWrite) {
		return stackerr.Newf(
			"write failed. Unable to write the key [%s]. Total space needed: [%d]",
			string(r.key),
			r.totalSize,
		)
	}

	return nil
}
