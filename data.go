package dyconf

type dataStore interface {
	save(key string, data []byte) (dataOffset, error)
	update(offset dataOffset, key string, data []byte) error
	fetch(offset dataOffset, key string) ([]byte, error)
}

type dataBlock struct {
	//maxDataBlockOffset dataOffset
	writeOffset dataOffset
	block       []byte
}

func (d *dataBlock) save(key string, data []byte) (dataOffset, error) {
	de := &dataElement{
		key:  string,
		data: data,
	}
}

type dataElement struct {
	totalSize uint32
	keySize   uint16
	key       string
	dataSize  uint32
	data      []byte
	next      dataOffset
}
