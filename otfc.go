package otfc

import (
	//"errors"
	"log"
	"os"
	"syscall"
	//"unsafe"
)

var configPtr *ConfigFile
var configMmap []byte

// Initializes the config.
func Init(fileName string) (err error) {
	configPtr, configMmap, err = InitConfigFile(fileName)
	return
}

func Shutdown() {
	if configPtr == nil {
		log.Println("Nil config pointer. Nothing to shutdown")
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

func Delete(key string) error {
	return configPtr.delete(key)
}
