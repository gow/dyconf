package dyconf

import (
	"os"
	"syscall"

	"github.com/facebookgo/stackerr"
)

func Init(fileName string) error {
	return defaultConfig.read_init(fileName)
}

func WriteInit(fileName string) error {
	return defaultConfig.write_init(fileName)
}

func Get(key string) ([]byte, error) {
	return defaultConfig.getBytes(key)
}

func Set(key string, value []byte) error {
	return defaultConfig.set(key, value)
}

func Close() error {
	return defaultConfig.close()
}

type config struct {
	fileName string
	file     *os.File
	block    []byte
}

var defaultConfig = &config{}

func (c *config) read_init(fileName string) error {
	c.fileName = fileName
	var err error
	c.file, err = os.Open(fileName)
	if err != nil {
		return stackerr.Newf("dyconf: failed to open the file [%s]. error: [%s]", fileName, err.Error())
	}
	// read lock the file
	if err = c.rlock(); err != nil {
		return err
	}
	defer c.unlock()

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

func (c *config) write_init(fileName string) error {
	c.fileName = fileName
	var err error
	c.file, err = os.Create(fileName)
	if err != nil {
		return stackerr.Newf("dyconf: failed to open the file [%s]. error: [%s]", fileName, err.Error())
	}

	// We now seek to the end of the file and write an empty byte. This is to bloat the file upto the
	// size we expect to mmap. If we don't do this mmap fails with the error "unexpected fault address"
	seekOffset, err := c.file.Seek(int64(defaultTotalSize), 0)
	if err != nil {
		return stackerr.Newf(
			"dyconf: failed to initialize for writing. Unexpected error occured while seeking to the "+
				"end [%#v] of the config file [%s]. error: [%s]",
			defaultTotalSize,
			fileName,
			err.Error(),
		)
	}
	if seekOffset != defaultTotalSize {
		return stackerr.Newf(
			"dyconf: failed to initialize for writing. Could not seek the file [%s] till the "+
				"required number of bytes [%#v]. Current seek offset: [%#v]",
			fileName,
			defaultTotalSize,
			seekOffset,
		)
	}
	_, err = c.file.Write([]byte("x"))
	if err != nil {
		return stackerr.Newf(
			"dyconf: failed to initialize for writing. Could not write the empty byte at the "+
				"end of the file [%s]. error: [%s]",
			fileName,
			err.Error(),
		)
	}

	// write lock the file
	if err = c.wlock(); err != nil {
		return err
	}
	defer c.unlock()

	// mmap
	c.block, err = syscall.Mmap(
		int(c.file.Fd()),
		0,
		int(defaultTotalSize),
		syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return stackerr.Newf("dyconf: failed to mmap the config file [%s]. error: [%s]", fileName, err.Error())
	}

	return nil
}
func (c *config) getBytes(key string) ([]byte, error) {
	// read lock the file
	if err := c.rlock(); err != nil {
		return nil, err
	}
	defer c.unlock()

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
	// write lock the file
	if err := c.wlock(); err != nil {
		return err
	}
	defer c.unlock()

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

func (c *config) rlock() error {
	if err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_SH); err != nil {
		return stackerr.Newf("dyconf: failed to acquire read lock for file [%s]. error: [%s]", c.file.Name(), err.Error())
	}
	return nil
}
func (c *config) wlock() error {
	if err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_EX); err != nil {
		return stackerr.Newf("dyconf: failed to acquire write lock for file [%s]. error: [%s]", c.file.Name(), err.Error())
	}
	return nil
}
func (c *config) unlock() error {
	if err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_UN); err != nil {
		return stackerr.Newf("dyconf: failed to release the lock for file [%s]. error: [%s]", c.file.Name(), err.Error())
	}
	return nil
}

func (c *config) close() error {
	return c.file.Close()
}
