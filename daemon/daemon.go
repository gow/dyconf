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
	cmdChannel chan string
}

func (daemon *otfcDaemon) init(fileName string) (err error) {
	daemon.configPtr, daemon.configMmap, err = config.NewWritable(fileName)
	if err != nil {
		return err
	}
	daemon.cmdChannel = make(chan string)
	daemon.server = &httpServer{configPtr: daemon.configPtr}
	err = daemon.server.start()
LOOP:
	for {
		log.Printf("Waiting for request")
		select {
		case <-time.After(30 * time.Second):
		case msg := <-daemon.cmdChannel:
			if msg == "STOP" {
				break LOOP
			} else {
				log.Println("Unknown message received: ", msg)
			}
		}
	}
	return err
}

func (daemon *otfcDaemon) stop() {
	daemon.cmdChannel <- "STOP"
}
