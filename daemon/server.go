package main

import (
	"encoding/json"
	"github.com/gow/otfc/config"
	"log"
	"net/http"
)

const (
	HTTP_PORT = "8088"
)

type otfcDaemon struct {
	configPtr  *config.ConfigFile
	configMmap []byte
}

func (daemon *otfcDaemon) init(fileName string) (err error) {
	daemon.configPtr, daemon.configMmap, err = config.NewWritable(fileName)
	if err != nil {
		return err
	}
	err = daemon.initServer()
	return err
}

func (daemon *otfcDaemon) initServer() error {
	http.HandleFunc(
		"/set",
		func(w http.ResponseWriter, r *http.Request) {
			daemon.httpCallbackSet(w, r)
		})
	err := http.ListenAndServe(":"+HTTP_PORT, nil)
	if err != nil {
		return err
	}
	return err
}

func (daemon *otfcDaemon) httpCallbackSet(
	w http.ResponseWriter,
	r *http.Request) {

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	log.Println("Key: ", key, "Value: ", value)
	if value == "" {
		sendHttpError(w, Error{ErrNo: ERR_DMN_INVALID_VALUE}, http.StatusNotAcceptable)
		return
	}
}

func sendHttpError(w http.ResponseWriter, err config.JSONable, errCode int) {
	jsonResponse, _ := json.Marshal(err.JSONableError())
	http.Error(w, string(jsonResponse), errCode)
}
