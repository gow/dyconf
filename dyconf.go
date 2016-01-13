package dyconf

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/facebookgo/stackerr"
)

func Init(fileName string) error {
	return defaultConfig.read_init(fileName)
}

func Get(key string) ([]byte, error) {
	return defaultConfig.getBytes(key)
}

func Close() error {
	return defaultConfig.Close()
}

type Config interface {
	Get(key string) ([]byte, error)
	Close() error
}

type ConfigManager interface {
	Set(key string, value []byte) error
	Delete(key string) error
	Close() error
}

type config struct {
	fileName string
	file     *os.File
	block    []byte
	initOnce sync.Once
}

func New(fileName string) (Config, error) {
	c := &config{}
	err := c.init(fileName)
	if err != nil {
		return nil, err
	}
	return c, nil
}

var defaultConfig = &config{}

type configManager struct {
	config
}

func NewManager(fileName string) (ConfigManager, error) {
	w := &configManager{}
	err := w.write_init(fileName)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (c *config) init(fileName string) error {
	var err error
	c.initOnce.Do(
		func() {
			err = c.read_init(fileName)
		},
	)
	return err
}

func (c *config) Get(key string) ([]byte, error) {
	return c.getBytes(key)
}

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
	// Key was not found in the index.
	if offset == 0 {
		return nil, stackerr.Newf("dyconf: key [%s] was not found", key)
	}

	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	data, found, err := db.fetch(offset, key)
	if err != nil {
		return nil, err
	}
	// Key was not found in the data block.
	if !found {
		return nil, stackerr.Newf("dyconf: key [%s] was not found", key)
	}

	return data, nil
}

func (c *configManager) create_new(fileName string) error {
	c.fileName = fileName
	var err error

	c.file, err = os.Create(fileName)
	if err != nil {
		return stackerr.Newf("dyconf: failed to create the file [%s]. error: [%s]", fileName, err.Error())
	}

	// write lock the file
	if err = c.wlock(); err != nil {
		return err
	}
	defer c.unlock()

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
	_, err = c.file.Write([]byte{0x00})
	if err != nil {
		return stackerr.Newf(
			"dyconf: failed to initialize for writing. Could not write the empty byte at the "+
				"end of the file [%s]. error: [%s]",
			fileName,
			err.Error(),
		)
	}
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

	// Save default header.
	h := &headerBlock{
		version:          123,
		totalSize:        defaultTotalSize,
		modifiedTime:     time.Now(),
		indexBlockOffset: headerBlockSize,
		indexBlockSize:   defaultIndexBlockSize,
		dataBlockOffset:  headerBlockSize + defaultIndexBlockSize,
		dataBlockSize:    defaultDataBlockSize,
		block:            c.block[0:headerBlockSize],
	}
	if err != h.save() {
		return err
	}

	return nil

}

func (c *configManager) write_init(fileName string) error {
	c.fileName = fileName
	var err error
	var existingFileSize int64

	stat, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return c.create_new(fileName)
	}

	c.file, err = os.OpenFile(fileName, os.O_RDWR, 0777)
	if err != nil {
		return stackerr.Newf("dyconf: failed to open the file [%s]. error: [%s]", fileName, err.Error())
	}
	existingFileSize = stat.Size()

	// write lock the file
	if err = c.wlock(); err != nil {
		return err
	}
	defer c.unlock()

	if existingFileSize != int64(defaultTotalSize) {
		return stackerr.Newf(
			"dyconf: failed to initialize the existing config file [%s]. The file size [%x] should be %x. "+
				"Either fix the file or delete it to discard all data and try again.",
			fileName,
			existingFileSize,
			defaultTotalSize,
		)
	}

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

func (c *configManager) Delete(key string) error {
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
	if offset == 0 {
		return nil // Key is not in the index. Nothing to delete.
	}

	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	newOffset, err := db.delete(offset, key)
	if err != nil {
		return err
	}

	// Save the offset if it's changed.
	if newOffset != offset {
		err = index.set(key, newOffset)
		if err != nil {
			return err
		}
	}

	// Update when the time when the config was modified.
	h.modifiedTime = time.Now()
	if err := h.save(); err != nil {
		return err
	}
	return nil
}

func (c *configManager) Set(key string, value []byte) error {
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
	if offset == 0 { // index was not found
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
		err = index.set(key, newOffset)
		if err != nil {
			return err
		}
	}

	// Update when the time when the config was modified.
	h.modifiedTime = time.Now()
	if err := h.save(); err != nil {
		return err
	}
	return nil
}

func (c *config) rlock() error {
	if err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_SH); err != nil {
		return stackerr.Newf("dyconf: failed to acquire read lock for file [%s]. error: [%s]", c.file.Name(), err.Error())
	}
	return nil
}
func (c *configManager) wlock() error {
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

func (c *config) Close() error {
	c.rlock()
	defer c.unlock()
	if err := syscall.Munmap(c.block); err != nil {
		return err
	}
	return c.file.Close()
}
