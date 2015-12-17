package dyconf

import (
	"os"
	"syscall"

	"github.com/facebookgo/stackerr"
)

func Init(fileName string) error {
	return defaultConfig.init(fileName)
}

func Get(key string) []byte {
	panic("Implement me!")
}

type config struct {
	fileName string
	file     *os.File
	block    []byte
}

var defaultConfig = &config{}

func (c *config) init(fileName string) error {
	c.fileName = fileName
	var err error
	c.file, err = os.Open(fileName)
	if err != nil {
		return stackerr.Newf("dyconf: failed to open the file [%s]. error: [%s]", fileName, err.Error())
	}
	// read lock the file
	if err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_SH); err != nil {
		return stackerr.Newf("dyconf: failed to acquire read lock for file [%s]. error: [%s]", fileName, err.Error())
	}
	defer syscall.Flock(int(c.file.Fd()), syscall.LOCK_UN)

	// mmap
	c.block, err = syscall.Mmap(
		int(c.file.Fd()),
		0,
		int(defaultTotalSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return stackerr.Newf("dyconf: failed to mmap the config file [%s]. error: [%s]", fileName, err.Error())
	}

	return nil
}

func (c *config) getBytes(key string) ([]byte, error) {
	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return nil, err
	}

	index := &indexBlock{
		count: defaultIndexCount,
		data:  c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
	}
	offset, err := index.get(key)
	if err != nil {
		return nil, err
	}

	if offset == 0 {
		return nil, stackerr.Newf("dyconf: key [%s] was not found in the index", key)
	}
	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	data, err := db.fetch(offset, key)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *config) set(key string, value []byte) error {
	var err error
	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return err
	}

	index := &indexBlock{
		count: defaultIndexCount,
		data:  c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
	}
	offset, err := index.get(key)
	if err != nil {
		return err
	}

	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	var newOffset = offset
	if offset != 0 { // index was not found
		newOffset, err = db.save(key, value)
		if err != nil {
			return err
		}
	} else {
		newOffset, err = db.update(offset, key, value)
		if err != nil {
			return err
		}
	}

	// Save the offset if it's changed.
	if newOffset != offset {
		err = index.set(key, offset)
		if err != nil {
			return err
		}

	}
	return nil
}
