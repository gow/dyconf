package main

import (
	"github.com/gow/otfc/config"
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
	daemon.server = &httpServer{daemon.configPtr}
	err = daemon.server.start()
	return err
}
