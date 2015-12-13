package dyconf

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/facebookgo/stackerr"
)

const (
	dataBlockHeaderSize = 0x10
)

type dataStore interface {
	save(key string, data []byte) (dataOffset, error)
	update(start dataOffset, key string, data []byte) (dataOffset, error)
	fetch(start dataOffset, key string) ([]byte, error)
}

type dataBlock struct {
	writeOffset dataOffset
	block       []byte
}

func (db *dataBlock) headerSize() dataOffset {
	return sizeOfUint32 * 4 // reserve 16 bytes for header use.
}

// save saves a new record and returns the offset where the record was saved.
func (db *dataBlock) save(key string, data []byte) (dataOffset, error) {
	if len(key) == 0 || len(data) == 0 {
		return 0, stackerr.Newf("dataBlock save failed. key [%s] and data [% x] must be non-zero length", key, data)
	}
	rec := &dataRecord{
		key:  []byte(key),
		data: data,
	}
	err := db.writeRecordTo(db.writeOffset, rec)
	if err != nil {
		return 0, err
	}

	// advance the write offset.
	writtenOffset := db.writeOffset
	db.writeOffset += dataOffset(rec.size())
	return writtenOffset, nil
}

func (db *dataBlock) fetch(start dataOffset, key string) ([]byte, error) {
	rec, _, _, err := db.find(start, key)
	if err != nil {
		return nil, err
	}

	if rec != nil { // record was found.
		return rec.data, nil
	}

	// record was not found.
	return nil, stackerr.Newf("dataBlock: key [%s] was not found starting at [%x]", key, start)
}

func (db *dataBlock) readRecordFrom(start dataOffset) (*dataRecord, error) {
	if start < db.headerSize() {
		return nil, stackerr.Newf(
			"dataBlock: invalid start offset [%#v]. Offsets between [%#v - %#v] is reserved for data block header",
			start,
			0,
			db.headerSize())
	}
	if start >= dataOffset(len(db.block)) {
		return nil, stackerr.Newf("dataBlock: Cannot read out of bound offset [%#v]. Block size: [%#v]", start, dataOffset(len(db.block)))
	}
	return (&dataRecord{}).read(db.block[start:])
}

func (db *dataBlock) writeRecordTo(start dataOffset, rec *dataRecord) error {
	if start < db.headerSize() {
		return stackerr.Newf(
			"dataBlock: invalid start offset [%#v]. Offsets between [%#v - %#v] is reserved for data block header",
			start,
			0,
			db.headerSize())
	}
	if start >= dataOffset(len(db.block)) {
		return stackerr.Newf("dataBlock: Cannot write to offset [%#v]. Block size: [%#v]", start, dataOffset(len(db.block)))
	}

	end := start + dataOffset(rec.size())
	if end > dataOffset(len(db.block)) {
		return stackerr.Newf(
			"dataBlock: Cannot write to offset [%#v]. Record [%#v bytes] exceeds data block boundary [%#v]",
			start,
			rec.size(),
			dataOffset(len(db.block)),
		)
	}
	err := rec.write(db.block[start:end])
	if err != nil {
		return stackerr.Newf("dataBlock[NEW]: Cannot write to offset [%#v]. Block state: \n%s\n Err: [%s]", start, spew.Sdump(db.block), err.Error())
	}
	return nil
}

func (db *dataBlock) find(start dataOffset, key string) (*dataRecord, dataOffset, dataOffset, error) {
	offset := start
	rec, err := db.readRecordFrom(offset)
	if err != nil {
		return nil, 0, 0, err
	}
	// The first record in the linked list matched.
	if bytes.Equal(rec.key, []byte(key)) {
		return rec, offset, 0, nil // previous offset will be 0 in this case.
	}

	// Iterate over the rest of the list.
	for rec.next != 0 {
		prevOffset := offset
		offset = rec.next
		rec, err = db.readRecordFrom(rec.next)
		if err != nil {
			return nil, 0, 0, err
		}
		if bytes.Equal(rec.key, []byte(key)) {
			return rec, offset, prevOffset, nil
		}
	}
	// The data record was not found. This is not an error. Return just a vald previous offset (the last record). This is so that the caller can take additional action when the record was not found.
	prevOffset := offset
	return nil, 0, prevOffset, nil
}

func (db *dataBlock) update(start dataOffset, key string, data []byte) (dataOffset, error) {
	rec, offset, prevOffset, err := db.find(start, key)
	if err != nil {
		return 0, err
	}

	// Case-1: The record is nil (not found). Just save a new record and adjust the previous record
	// to point to the newly added record. There will always be a previous record.
	if rec == nil {
		offset, err := db.save(key, data)
		if err != nil {
			return 0, err
		}
		// There was a previous record. Fetch and update it.
		// Since the start of the linked list hasn't changed, return the same value.
		prevRec, err := db.readRecordFrom(prevOffset)
		if err != nil {
			return 0, err
		}
		prevRec.next = offset
		err = db.writeRecordTo(prevOffset, prevRec)
		if err != nil {
			return 0, err
		}
		return start, nil
	}

	// Case-2. Record was found. But The new data is not an exact fit. So, add a new record and adjust
	// previous record if required.
	if len(rec.data) != len(data) {
		// Save the new data in the record and rewrite it at writeOffset
		rec.data = data
		if err := db.writeRecordTo(db.writeOffset, rec); err != nil {
			return 0, err
		}
		offset = db.writeOffset
		db.writeOffset += dataOffset(rec.size()) // advance the write pointer.

		// If there was no previous record, then this was the first record.
		// It was moved because it didn't fit in it's previous offset. Return it's new offset.
		if prevOffset == 0 {
			return offset, nil
		}

		// There was a previous record. Fetch and update it.
		// Since the start of the linked list hasn't changed, return the same value.
		prevRec, err := db.readRecordFrom(prevOffset)
		if err != nil {
			return 0, err
		}
		prevRec.next = offset
		err = db.writeRecordTo(prevOffset, prevRec)
		if err != nil {
			return 0, err
		}
		return start, nil
	}

	// Case-3: The record was found and the new data is an exact fit in the current space.
	rec.data = data
	if err := db.writeRecordTo(offset, rec); err != nil {
		return 0, err
	}

	return start, nil
}

type record interface {
	read([]byte) (*dataRecord, error)
	write([]byte)
	size() uint32
}

type dataRecord struct {
	key  []byte
	data []byte
	next dataOffset
}

func (r *dataRecord) read(block []byte) (*dataRecord, error) {
	buf := bytes.NewReader(block)

	// read key size.
	var keySize uint32
	err := binary.Read(buf, binary.LittleEndian, &keySize)
	if err != nil {
		return nil, stackerr.Newf("dataRecord: failed to read the key size. error: [%s]", err.Error())
	}

	// read data size.
	var dataSize uint32
	err = binary.Read(buf, binary.LittleEndian, &dataSize)
	if err != nil {
		return nil, stackerr.Newf("dataRecord: failed to read the data size. error: [%s]. Block: \n%s\n", err.Error(), spew.Sdump(block))
	}

	// allocate key and then read into it.
	r.key = make([]byte, keySize)
	err = binary.Read(buf, binary.LittleEndian, &r.key)
	if err != nil {
		return nil, stackerr.Newf("dataRecord: failed to read the key. error: [%s]. Block: \n%s\n", err.Error(), spew.Sdump(block))
	}

	// allocate data and then read into it.
	r.data = make([]byte, dataSize)
	err = binary.Read(buf, binary.LittleEndian, &r.data)
	if err != nil {
		return nil, stackerr.Newf("dataRecord: failed to read the data. error: [%s]. Block: \n%s\n", err.Error(), spew.Sdump(block))
	}

	// Finally read the next pointer.
	err = binary.Read(buf, binary.LittleEndian, &r.next)
	if err != nil {
		return nil, stackerr.Newf("dataRecord: failed to read the next pointer. error: [%s]. Block: \n%s\n", err.Error(), spew.Sdump(block))
	}

	return r, nil
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
