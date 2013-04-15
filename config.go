package otfc

import (
	"log"
)

const (
	CONFIG_VERSION           = 1
	CONFIG_FILE_SIZE         = 294912
	DATA_BLOCK_SIZE          = 256 * 1024 // 256k bytes
	DEFAULT_CONFIG_FILE_NAME = "/tmp/71ebdf319f2a7fa1d4eb45f9c4b7cf64"
)

type Config struct {
	header configHeader
	index  indexBlock
	data   [DATA_BLOCK_SIZE]byte
}

// Sets the given config key and value pair.
func (configPtr *Config) set(key string, value []byte) (err error) {
	count := configPtr.header.NumRecords()
	dataLength := uint32(len(value))
	indexPtr := &(configPtr.index)
	indexPtr.set(key, configPtr.header.writeOffset, dataLength)

	// Copy the data
	bytesCopied := copy(configPtr.data[configPtr.header.writeOffset:], value)
	log.Printf("Data copied: [%d] bytes\n", bytesCopied)

	log.Println("WriteOffset: ", configPtr.header.writeOffset)
	configPtr.header.writeOffset = configPtr.header.writeOffset + dataLength
	log.Println("WriteOffset: ", configPtr.header.writeOffset)
	configPtr.header.SetRecordCount(count + 1)
	return
}

func (configPtr *Config) get(key string) (value []byte, err error) {
	offset, length, err := configPtr.index.get(key)
	if err != nil {
		return
	}
	value = configPtr.data[offset:(offset + length)]
	return value, nil
}
