package otfc

import (
	//"errors"
	"log"
	//"os"
	"github.com/gow/otfc/config"
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
	//val, _ := configPtr.get(key)
	//log.Printf("Value from config.get(%s): [%s]", key, val)
	return configPtr.Get(key)
}

func Delete(key string) error {
	return configPtr.Delete(key)
}
