package dyconf

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/facebookgo/stackerr"
)

// Config provides methods to access the config values.
type Config interface {
	Get(key string) ([]byte, error)
	Close() error
}

// ConfigManager provides methods to manage the config data.
type ConfigManager interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
	Map() (map[string][]byte, error)
	Defrag() error
	Close() error

	// unexported
	freeDataByteCount() (uint32, error)
	dataBlockSize() (uint32, error)
}

type config struct {
	fileName string
	file     *os.File
	block    []byte
	initOnce sync.Once
}

// New initializes and returns a new config that can be used to get the config values.
func New(fileName string) (Config, error) {
	c := &config{}
	err := c.init(fileName)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type configManager struct {
	config
}

// NewManager initializes and returns a new ConfigManager that can be used to manage the config data.
func NewManager(fileName string) (ConfigManager, error) {
	w := &configManager{}
	err := w.writeInit(fileName)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (c *config) Get(key string) ([]byte, error) {
	return c.getBytes(key)
}

func (c *config) init(fileName string) error {
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
		size: defaultIndexCount,
		data: c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
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

func (c *configManager) createNew(fileName string) error {
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

	// We now seek to the end of the file and write an empty byte. This is to bloat the file up to the
	// size we expect to mmap. If we don't do this mmap fails with the error "unexpected fault address"
	seekOffset, err := c.file.Seek(int64(defaultTotalSize-1), 0)
	if err != nil {
		return stackerr.Newf(
			"dyconf: failed to initialize for writing. Unexpected error occured while seeking to the "+
				"end [%#v] of the config file [%s]. error: [%s]",
			defaultTotalSize,
			fileName,
			err.Error(),
		)
	}
	if seekOffset != defaultTotalSize-1 {
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

func (c *configManager) writeInit(fileName string) error {
	c.fileName = fileName
	var err error
	var existingFileSize int64

	stat, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return c.createNew(fileName)
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
		size: defaultIndexCount,
		data: c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
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
	return c.setNoLock(key, value)
}

// setNoLock is a helper method to set the key-value in the config. It does so without locking the file.
// So, it should always be used in a method that locks the file.
func (c *configManager) setNoLock(key string, value []byte) error {
	var err error
	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return err
	}

	index := &indexBlock{
		size: defaultIndexCount,
		data: c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
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

func (c *configManager) Map() (map[string][]byte, error) {
	// read lock the file
	if err := c.rlock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	ret := make(map[string][]byte)
	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return nil, err
	}

	index := &indexBlock{
		size: defaultIndexCount,
		data: c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
	}
	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}

	offsets, err := index.getAll()
	if err != nil {
		return nil, err
	}

	for _, offset := range offsets {
		kv, err := db.fetchAll(offset)
		if err != nil {
			return nil, err
		}
		for key, val := range kv {
			ret[key] = val
		}
	}
	return ret, nil
}

func (c *configManager) Defrag() error {
	// Save the current values. Don't lock before calling Map() since it also takes a read lock.
	kvMap, err := c.Map()
	if err != nil {
		return err
	}

	// now write lock the file
	if err := c.wlock(); err != nil {
		return err
	}
	defer c.unlock()

	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return err
	}

	index := &indexBlock{
		size: defaultIndexCount,
		data: c.block[h.indexBlockOffset : uint32(h.indexBlockOffset)+h.indexBlockSize],
	}
	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}

	// Reset the index and the data block.
	if err := index.reset(); err != nil {
		return err
	}
	if err := db.reset(); err != nil {
		return err
	}

	for k, v := range kvMap {
		if err := c.Set(k, v); err != nil {
			return stackerr.Wrap(err)
		}
	}
	return nil
}

func (c *configManager) freeDataByteCount() (uint32, error) {
	// read lock the file
	if err := c.rlock(); err != nil {
		return 0, err
	}
	defer c.unlock()

	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return 0, err
	}

	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	return db.freeByteCount()
}

func (c *configManager) dataBlockSize() (uint32, error) {
	// read lock the file
	if err := c.rlock(); err != nil {
		return 0, err
	}
	defer c.unlock()

	h, err := (&headerBlock{}).read(c.block[0:headerBlockSize])
	if err != nil {
		return 0, err
	}

	db := &dataBlock{block: c.block[h.dataBlockOffset : uint32(h.dataBlockOffset)+h.dataBlockSize]}
	return db.size()
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
