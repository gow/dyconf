package otfc

import (
	"fmt"
	//"log"
)

const (
	CONFIG_VERSION   = 1
	CONFIG_FILE_SIZE = 294912
)

type Config struct {
	header configHeader
	index  indexBlock
	data   dataBlock
}

// Sets the given config key and value pair.
func (configPtr *Config) set(key string, value []byte) (err error) {
	// Check if the key already exists
	if _, err := configPtr.get(key); err == nil {
		return ConfigError{
			ERR_CONFIG_SET_EXISTING_KEY,
			fmt.Sprintf("key [%s]", key)}
	}

	count := configPtr.header.NumRecords()
	dataLength := uint32(len(value))
	indexPtr := &(configPtr.index)
	err = indexPtr.set(key, configPtr.header.writeOffset, dataLength)
	if err != nil {
		return
	}

	// Copy the data
	newOffset, err := configPtr.data.set(configPtr.header.writeOffset, value)
	if err != nil {
		return
	}
	if newOffset == 0 {
		return ConfigError{}
	}

	configPtr.header.writeOffset = configPtr.header.writeOffset + dataLength
	configPtr.header.SetRecordCount(count + 1)
	return nil
}

func (configPtr *Config) get(key string) (value []byte, err error) {
	offset, length, err := configPtr.index.get(key)
	if err != nil {
		return
	}
	return configPtr.data.get(offset, length)
}

func (configPtr *Config) delete(key string) error {
	return configPtr.index.delete(key)
}
