package dyconf

import (
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
)

func WriteTestConfig() error {

	c := defaultConfig
	fmt.Println("Block length: ", len(c.block))
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
	if err := h.save(); err != nil {
		return err
	}
	return nil
}

func ReadTestConfig(fileName string) {
	c := &readConfig{}
	if err := c.read_init(fileName); err != nil {
		panic(err)
	}

	h, err := (&headerBlock{}).read(c.block[:headerBlockSize])
	if err != nil {
		panic(err)
	}
	spew.Dump("Read header: ", h)
}
