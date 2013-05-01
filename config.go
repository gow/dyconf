package otfc

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

const (
	CONFIG_VERSION   = 1
	CONFIG_FILE_SIZE = 294912
)

type ConfigFile struct {
	header configHeader
	index  indexBlock
	data   dataBlock
}

// Initializes the config.
func InitConfigFile(
	fileName string) (configPtr *ConfigFile, configMmap []byte, err error) {

	mapFile, err := createFile(fileName, CONFIG_FILE_SIZE)
	//mapFile, err := os.Open(fileName)
	if err != nil {
		return
	}
	// mmap the config file.
	configMmap, err = syscall.Mmap(
		int(mapFile.Fd()),
		0,
		int(CONFIG_FILE_SIZE),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
	if err != nil {
		mapFile.Close()
		return
	}
	// Make sure mmap gave us enough memory.
	if len(configMmap) < int(CONFIG_FILE_SIZE) {
		err = errors.New("Insufficient memmory")
		mapFile.Close()
		return
	}
	// Convert the byte array to Config struct type.
	configPtr = (*ConfigFile)(unsafe.Pointer(&configMmap[0]))

	if configPtr.header.Version() < uint16(1) {
		configPtr.header.SetVersion(uint16(CONFIG_VERSION))
	}
	return
}

// Sets the given config key and value pair.
func (configPtr *ConfigFile) set(key string, value []byte) (err error) {
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

func (configPtr *ConfigFile) get(key string) (value []byte, err error) {
	offset, length, err := configPtr.index.get(key)
	if err != nil {
		return
	}
	return configPtr.data.get(offset, length)
}

func (configPtr *ConfigFile) delete(key string) error {
	return configPtr.index.delete(key)
}
