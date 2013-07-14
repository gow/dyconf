package main

import (
	"encoding/json"
	"fmt"
	"github.com/gow/dyconf/config"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	HTTP_PORT = "8088"
)

type httpServer struct {
	configPtr *config.ConfigFile
	listener  net.Listener
	mutex     sync.Mutex
}

func (server *httpServer) start() (err error) {
	// Setup handler functions
	http.HandleFunc(
		"/set",
		func(w http.ResponseWriter, r *http.Request) {
			server.mutex.Lock()
			defer server.mutex.Unlock()
			server.httpCallbackSet(w, r)
		})
	http.HandleFunc(
		"/get",
		func(w http.ResponseWriter, r *http.Request) {
			server.mutex.Lock()
			defer server.mutex.Unlock()
			server.httpCallbackGet(w, r)
		})
	http.HandleFunc(
		"/delete",
		func(w http.ResponseWriter, r *http.Request) {
			server.mutex.Lock()
			defer server.mutex.Unlock()
			server.httpCallbackDelete(w, r)
		})
	// Open TCP port
	server.listener, err = net.Listen("tcp", ":"+HTTP_PORT)
	if err != nil {
		return err
	}

	// Create a HTTP server
	s := &http.Server{
		Addr:           ":" + HTTP_PORT,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 0,
	}
	go s.Serve(server.listener)
	if err != nil {
		return err
	}
	return err
}

func (server *httpServer) stop() error {
	log.Println("Stopping the server")
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.listener.Close()
}

func (server *httpServer) httpCallbackGet(
	resp http.ResponseWriter,
	req *http.Request) {

	key := req.URL.Query().Get("key")
	value, err := server.configPtr.Get(key)
	if err != nil {
		sendHttpError(resp, err.(config.Error), http.StatusBadRequest)
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

func (server *httpServer) httpCallbackSet(
	resp http.ResponseWriter,
	req *http.Request) {

	key := req.URL.Query().Get("key")
	value := req.URL.Query().Get("value")
	log.Println("Key: ", key, "Value: ", value)
	if value == "" {
		sendHttpError(
			resp,
			config.Error{ErrNo: ERR_DMN_INVALID_VALUE},
			http.StatusNotAcceptable)
		return
	}
	err := server.configPtr.Set(key, []byte(value))
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

func (server *httpServer) httpCallbackDelete(
	resp http.ResponseWriter,
	req *http.Request) {

	key := req.URL.Query().Get("key")
	err := server.configPtr.Delete(key)
	log.Println("httpCallbackDelete [key, err]: ", key, err)
	if err != nil {
		sendHttpError(resp, err.(config.Error), http.StatusBadRequest)
		return
	}
	sendHttpJSONResponse(
		resp,
		struct {
			Status string
			Key    string
		}{"OK", key})
}

func sendHttpError(w http.ResponseWriter, err config.Error, errCode int) {
	response := struct {
		Status string
		Err    config.Error
	}{"error", err}
	jsonResponse, _ := json.Marshal(response)
	http.Error(w, string(jsonResponse), errCode)
}

func sendHttpJSONResponse(w http.ResponseWriter, data interface{}) {
	jsonResponse, _ := json.Marshal(data)
	fmt.Fprintf(w, string(jsonResponse)+"\n")
}
