package dyconf

import "github.com/davecgh/go-spew/spew"

func WriteTestConfig() []byte {

	b := make([]byte, headerBlockSize)
	h := &headerBlock{
		version:          123,
		totalSize:        defaultTotalSize,
		modifiedTime:     0x80809090,
		indexBlockOffset: 0x44444444,
		indexBlockSize:   defaultIndexBlockSize,
		dataBlockOffset:  0x88888888,
		dataBlockSize:    defaultDataBlockSize,
		block:            b,
	}
	if err := h.save(); err != nil {
		panic(err)
	}
	return b
}

func ReadTestConfig(fileName string) {
	c := &config{}
	if err := c.init(fileName); err != nil {
		panic(err)
	}

	h, err := (&headerBlock{}).read(c.block[:headerBlockSize])
	if err != nil {
		panic(err)
	}
	spew.Dump("Read header: ", h)
}
