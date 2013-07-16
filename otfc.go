package otfc

import (
	"github.com/gow/dyconf/config"
	"log"
	"syscall"
)

var configPtr *config.ConfigFile
var configMmap []byte

// Initializes the config.
func Init(fileName string) (err error) {
	configPtr, configMmap, err = config.NewWritable(fileName)
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
	configPtr.PrintHeaderBlock()
	configPtr.PrintIndexBlock()
	log.Println("\n==================================================\n")
}

func Set(key string, value []byte) error {
	return configPtr.Set(key, value)
}

func Get(key string) ([]byte, error) {
	return configPtr.Get(key)
}

func Delete(key string) error {
	return configPtr.Delete(key)
}
