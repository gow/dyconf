package otfc

import (
	"log"
	//"fmt"
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var configPtr *Config
var configMmap []byte

// Initializes the config.
func Init(fileName string) (err error) {
	log.Printf("Config File: %s", fileName)
	mapFile, err := createFile(fileName, CONFIG_FILE_SIZE)
	//mapFile, err := os.Open(fileName)
	if err != nil {
		return
	}
	// mmap the config file.
	configMmap, err := syscall.Mmap(
		int(mapFile.Fd()),
		0,
		int(CONFIG_FILE_SIZE),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
	if err != nil {
		mapFile.Close()
		log.Printf("%s\n", err)
		return
	}
	// Make sure mmap gave us enough memory.
	if len(configMmap) < int(CONFIG_FILE_SIZE) {
		err = errors.New("Insufficient memmory")
		mapFile.Close()
		return
	}
	// Convert the byte array to Config struct type.
	configPtr = (*Config)(unsafe.Pointer(&configMmap[0]))

	if configPtr.header.Version() < uint16(1) {
		configPtr.header.SetVersion(uint16(CONFIG_VERSION))
	}
	return
}

func Shutdown() {
	if configPtr == nil {
		log.Println("Nil config patr. Nothing to shutdown")
		return
	}
	syscall.Munmap(configMmap)
}

func Print() {
	log.Println("\n==================================================\n")
	PrintHeaderBlock()
	PrintIndexBlock()
	log.Println("\n==================================================\n")
}

func PrintIndexBlock() {
	configPtr.index.print()
}

func PrintHeaderBlock() {
	configPtr.header.print()
}

func createFile(fileName string, size int32) (file *os.File, err error) {
	file, err = os.Create(fileName)
	if err != nil {
		return
	}
	_, err = file.Seek(int64(size), 0)
	if err != nil {
		log.Println(err)
		return
	}

	_, err = file.Write([]byte("x"))
	if err != nil {
		log.Println(err)
		return
	}
	return
}

func Set(key string, value []byte) error {
	return configPtr.set(key, value)
}

func Get(key string) ([]byte, error) {
	//val, _ := configPtr.get(key)
	//log.Printf("Value from config.get(%s): [%s]", key, val)
	return configPtr.get(key)
}
