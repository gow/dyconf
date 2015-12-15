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
