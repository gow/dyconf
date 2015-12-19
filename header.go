package dyconf

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/facebookgo/stackerr"
)

const (
	headerBlockSize       = 0x20              // 32 bytes
	defaultIndexBlockSize = 1024 * 1024 * 4   // 4 MB
	defaultDataBlockSize  = 1024 * 1024 * 128 // 128 MB
	defaultTotalSize      = headerBlockSize + defaultIndexBlockSize + defaultDataBlockSize
	defaultIndexCount     = defaultIndexBlockSize / sizeOfUint32

	// Max limits
	maxIndexBlockSize = 1024 * 1024 * 128  // 128 MB
	maxDataBlockSize  = 1024 * 1024 * 1024 // 1 GB
)

type headerBlock struct {
	version          uint32
	totalSize        uint32
	modifiedTime     time.Time
	indexBlockOffset dataOffset
	indexBlockSize   uint32
	dataBlockOffset  dataOffset
	dataBlockSize    uint32

	block []byte
}

func (h *headerBlock) read(block []byte) (*headerBlock, error) {
	if len(block) < headerBlockSize {
		return nil, stackerr.Newf(
			"headerBlock: failed to read the header. It should be [%#v] bytes. Given block: \n%s\n",
			headerBlockSize,
			spew.Sdump(block),
		)
	}

	h.block = block
	buf := bytes.NewReader(block)
	if err := binary.Read(buf, binary.LittleEndian, &h.version); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the version. error: [%s]", err.Error())
	}

	if err := binary.Read(buf, binary.LittleEndian, &h.totalSize); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the total size. error: [%s]", err.Error())
	}

	var timestamp int64
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the modified time. error: [%s]", err.Error())
	}
	h.modifiedTime = time.Unix(timestamp, 0)

	if err := binary.Read(buf, binary.LittleEndian, &h.indexBlockOffset); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the index block offset. error: [%s]", err.Error())
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.indexBlockSize); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the index block size. error: [%s]", err.Error())
	}
	if h.indexBlockSize > maxIndexBlockSize {
		return nil, stackerr.Newf("headerBlock: invalid index block size [%#v]. It should not exceed [%#v]", h.indexBlockSize, maxIndexBlockSize)
	}

	if err := binary.Read(buf, binary.LittleEndian, &h.dataBlockOffset); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the data block offset. error: [%s]", err.Error())
	}
	if err := binary.Read(buf, binary.LittleEndian, &h.dataBlockSize); err != nil {
		return nil, stackerr.Newf("headerBlock: failed to read the data block size. error: [%s]", err.Error())
	}
	if h.dataBlockSize > maxDataBlockSize {
		return nil, stackerr.Newf("headerBlock: invalid data block size [%#v]. It should not exceed [%#v]", h.dataBlockSize, maxDataBlockSize)
	}
	return h, nil
}

func (h *headerBlock) save() error {
	if len(h.block) < headerBlockSize {
		return stackerr.Newf(
			"headerBlock: failed to save the header. It should be [%#v] bytes. Given block: \n%s\n",
			headerBlockSize,
			spew.Sdump(h.block),
		)
	}

	timestamp := h.modifiedTime.Unix()

	buf := &writeBuffer{buf: h.block}
	binary.Write(buf, binary.LittleEndian, h.version)
	binary.Write(buf, binary.LittleEndian, h.totalSize)
	binary.Write(buf, binary.LittleEndian, timestamp)
	binary.Write(buf, binary.LittleEndian, h.indexBlockOffset)
	binary.Write(buf, binary.LittleEndian, h.indexBlockSize)
	binary.Write(buf, binary.LittleEndian, h.dataBlockOffset)
	binary.Write(buf, binary.LittleEndian, h.dataBlockSize)

	if buf.err != nil {
		return stackerr.Newf("headerBlock: unable to write the header. Details: [%s]", buf.err.Error())
	}

	return nil
}
