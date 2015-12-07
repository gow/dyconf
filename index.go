package dyconf

import "hash/fnv"

const (
	defaultIndexSize = 1 << 20 // 1 million
)

type dataPtr uint32

type index interface {
	get(key string) (dataPtr, error)
	set(key string, ptr dataPtr) error
}

type indexBlock struct {
	size uint32 // current size of the index.
	data []byte // Index data block
}

func (i *indexBlock) get(key string) (dataPtr, error) {
	panic("Implement me!")
	return 0, nil
}

func (i *indexBlock) set(key string, ptr dataPtr) error {
	panic("Implement me!")
	return nil
}

func hash(key string) (uint32, error) {
	h := fnv.New32a() // Use FNV-1a hashing.
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
