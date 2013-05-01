package config

import (
	"log"
	"os"
)

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

func (configPtr *ConfigFile) PrintIndexBlock() {
	configPtr.index.print()
}

func (configPtr *ConfigFile) PrintHeaderBlock() {
	configPtr.header.print()
}
