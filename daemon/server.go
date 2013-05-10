package main

import (
	"encoding/json"
	"fmt"
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
	http.HandleFunc(
		"/get",
		func(w http.ResponseWriter, r *http.Request) {
			daemon.httpCallbackGet(w, r)
		})
	err := http.ListenAndServe(":"+HTTP_PORT, nil)
	if err != nil {
		return err
	}
	return err
}

func (daemon *otfcDaemon) httpCallbackGet(
	resp http.ResponseWriter,
	req *http.Request) {

	key := req.URL.Query().Get("key")
	value, err := daemon.configPtr.Get(key)
	if err != nil {
		sendHttpError(resp, err, http.StatusBadRequest)
		return
	}
	sendHttpJSONResponse(
		resp,
		struct {
			Status string
			Key    string
			Value  []byte
		}{"OK", key, value})
}

func (daemon *otfcDaemon) httpCallbackSet(
	resp http.ResponseWriter,
	req *http.Request) {

	key := req.URL.Query().Get("key")
	value := req.URL.Query().Get("value")
	log.Println("Key: ", key, "Value: ", value)
	if value == "" {
		sendHttpError(
			resp,
			Error{ErrNo: ERR_DMN_INVALID_VALUE},
			http.StatusNotAcceptable)
		return
	}
	err := daemon.configPtr.Set(key, []byte(value))
	if err != nil {
		sendHttpJSONResponse(resp, err)
		return
	}
	sendHttpJSONResponse(
		resp,
		struct {
			Status string
			Key    string
			Value  string
		}{"OK", key, value})
}

func sendHttpError(w http.ResponseWriter, err interface{}, errCode int) {
	response := struct {
		Status string
		Err    interface{}
	}{"error", err}
	jsonResponse, _ := json.Marshal(response)
	http.Error(w, string(jsonResponse)+"\n", errCode)
}

func sendHttpJSONResponse(w http.ResponseWriter, data interface{}) {
	jsonResponse, _ := json.Marshal(data)
	fmt.Fprintf(w, string(jsonResponse)+"\n")
}
