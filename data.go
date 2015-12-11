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
	writeOffset dataOffset
	block       []byte
}

// save saves a new record and returns the offset where the record was saved.
func (db *dataBlock) save(key string, data []byte) (dataOffset, error) {
	if len(key) == 0 || len(data) == 0 {
		return 0, stackerr.Newf("dataBlock save failed. key [%s] and data [% x] must be non-zero length", key, data)
	}
	r := &dataRecord{
		key:  []byte(key),
		data: data,
	}
	err := r.write(db.block[db.writeOffset:])
	if err != nil {
		return 0, err
	}

	// advance the write offset.
	writtenOffset := db.writeOffset
	db.writeOffset += dataOffset(r.size())
	return writtenOffset, nil
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

func (r *dataRecord) read(block []byte) error {
	buf := bytes.NewReader(block)

	// read key size.
	var keySize uint32
	err := binary.Read(buf, binary.LittleEndian, &keySize)
	if err != nil {
		return stackerr.Newf("dataRecord: failed to read the key size. error: [%s]", err.Error())
	}

	// read data size.
	var dataSize uint32
	err = binary.Read(buf, binary.LittleEndian, &dataSize)
	if err != nil {
		return stackerr.Newf("dataRecord: failed to read the data size. error: [%s]", err.Error())
	}

	// allocate key and then read into it.
	r.key = make([]byte, keySize)
	err = binary.Read(buf, binary.LittleEndian, &r.key)
	if err != nil {
		return stackerr.Newf("dataRecord: failed to read the key. error: [%s]", err.Error())
	}

	// allocate data and then read into it.
	r.data = make([]byte, dataSize)
	err = binary.Read(buf, binary.LittleEndian, &r.data)
	if err != nil {
		return stackerr.Newf("dataRecord: failed to read the data. error: [%s]", err.Error())
	}

	// Finally read the next pointer.
	err = binary.Read(buf, binary.LittleEndian, &r.next)
	if err != nil {
		return stackerr.Newf("dataRecord: failed to read the next pointer. error: [%s]", err.Error())
	}

	return nil
}

func (r *dataRecord) write(block []byte) error {
	if r.size() > uint32(len(block)) {
		return stackerr.Newf("Unable to write the key [%s]. bytes available: [%d], needed: [%d].", string(r.key), len(block), r.size())
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
