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
	key  []byte
	data []byte
	next dataOffset
}

type writeBuffer struct {
	err  error
	wPtr int
	buf  []byte
}

func (b *writeBuffer) Write(data []byte) (int, error) {
	// Return if there was any previous error.
	if b.err != nil {
		return 0, b.err
	}

	l := len(data)
	if l == 0 { // Early return.
		return 0, nil
	}

	c := copy(b.buf[b.wPtr:], data)
	b.wPtr += c

	if c != l {
		b.err = fmt.Errorf("copied [%d] of [%d] bytes. Data: [% x]", c, l, data)
	}
	return c, b.err
}

func (r *dataRecord) read(block []byte) error {
	buf := bytes.NewReader(block)

	// read key size.
	var keySize uint32
	err := binary.Read(buf, binary.LittleEndian, &keySize)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// read data size.
	var dataSize uint32
	err = binary.Read(buf, binary.LittleEndian, &dataSize)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// allocate key and then read into it.
	r.key = make([]byte, keySize)
	err = binary.Read(buf, binary.LittleEndian, &r.key)
	if err != nil {
		return stackerr.Wrap(err)
	}

	// allocate data and then read into it.
	r.data = make([]byte, dataSize)
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

func (r *dataRecord) write(block []byte) error {
	if r.size() > uint32(len(block)) {
		return stackerr.Newf("Unable to write the key [%s]. [%d] bytes available out of [%d] needed.", string(r.key), len(block), r.size())
	}
	buf := &writeBuffer{buf: block}
	// Just write in one order. The error if any will be caught and cached in buf.Write
	//binary.Write(buf, binary.LittleEndian, r.size())
	binary.Write(buf, binary.LittleEndian, r.keySize())
	binary.Write(buf, binary.LittleEndian, r.dataSize())
	binary.Write(buf, binary.LittleEndian, r.key)
	binary.Write(buf, binary.LittleEndian, r.data)
	binary.Write(buf, binary.LittleEndian, r.next)

	// Check if there any error in writing.
	if buf.err != nil {
		return stackerr.Newf("Unable to write the key [%s]. Total space needed: [%d]. Details: %s", string(r.key), r.size(), buf.err.Error())
	}
	return nil
}

func (r *dataRecord) size() uint32 {
	size := uint32(0)
	size += sizeOfUint32        // keySize field
	size += sizeOfUint32        // dataSize field
	size += uint32(len(r.key))  // key field
	size += uint32(len(r.data)) // data field
	size += sizeOfUint32        // next field
	return size
}

func (r *dataRecord) keySize() uint32 {
	return uint32(len(r.key))
}

func (r *dataRecord) dataSize() uint32 {
	return uint32(len(r.data))
}
