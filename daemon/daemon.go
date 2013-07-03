package main

import (
	"github.com/gow/otfc/config"
	"log"
	"time"
)

type otfcDaemon struct {
	configPtr  *config.ConfigFile
	configMmap []byte
	server     *httpServer
}

func (daemon *otfcDaemon) init(fileName string) (err error) {
	daemon.configPtr, daemon.configMmap, err = config.NewWritable(fileName)
	if err != nil {
		return err
	}
	daemon.server = &httpServer{configPtr: daemon.configPtr}
	err = daemon.server.start()
	for {
		log.Printf("Waiting for request")
		<-time.After(30 * time.Second)
	}
	return err
}
