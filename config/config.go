package config

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

const (
	VERSION   = 1
	FILE_SIZE = 294912
)

type ConfigFile struct {
	header configHeader
	index  indexBlock
	data   dataBlock
}

func New(
	fileName string) (configPtr *ConfigFile, configMmap []byte, err error) {
  return initWithProtOptions(fileName, syscall.PROT_READ)
}
func NewWritable(
	fileName string) (configPtr *ConfigFile, configMmap []byte, err error) {
  return initWithProtOptions(fileName, syscall.PROT_READ|syscall.PROT_WRITE)
}

// Initializes the config.
func initWithProtOptions(
	fileName string,
  prot int) (configPtr *ConfigFile, configMmap []byte, err error) {

	mapFile, err := createFile(fileName, FILE_SIZE)
	//mapFile, err := os.Open(fileName)
	if err != nil {
		return
	}
	// mmap the config file.
	configMmap, err = syscall.Mmap(
		int(mapFile.Fd()),
		0, // offset
		int(FILE_SIZE),
    prot,
		syscall.MAP_SHARED)
	if err != nil {
		mapFile.Close()
		return
	}
	// Make sure mmap gave us enough memory.
	if len(configMmap) < int(FILE_SIZE) {
		err = errors.New("Insufficient memmory")
		mapFile.Close()
		return
	}
	// Convert the byte array to Config struct type.
	configPtr = (*ConfigFile)(unsafe.Pointer(&configMmap[0]))

	if configPtr.header.Version() < uint16(1) {
		configPtr.header.SetVersion(uint16(VERSION))
	}
	return
}

// Sets the given config key and value pair.
func (configPtr *ConfigFile) Set(key string, value []byte) (err error) {
	// Check if the key already exists
	if _, err := configPtr.Get(key); err == nil {
		return Error{
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
		return Error{}
	}

	configPtr.header.writeOffset = configPtr.header.writeOffset + dataLength
	configPtr.header.SetRecordCount(count + 1)
	return nil
}

func (configPtr *ConfigFile) Get(key string) (value []byte, err error) {
	offset, length, err := configPtr.index.get(key)
	if err != nil {
		return
	}
	return configPtr.data.get(offset, length)
}

func (configPtr *ConfigFile) Delete(key string) error {
	return configPtr.index.delete(key)
}
