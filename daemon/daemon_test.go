package main

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"
)

type testRequest struct {
	reqType string
	key     string
	value   string
}
type daemonTestCase struct {
	requestSequence []testRequest
	finalResponse   string
}

var testCases = []daemonTestCase{
	// Empty get
	{
		[]testRequest{{"get", "123", ""}},
		`{"Status":"error","Err":{"ErrNo":2002,"ErrMsg":"index key not found. key [123]"}}`,
	},
	// Set
	// TODO: Fix "panic: http: multiple registrations for /set" error
	/*
	  {
	    []testRequest{{"set", "qqq", "QWErty~~!!"}},
	    `{"Status":"OK","Key":"qqq","Value":"QWErty~~!!"}`,
	  },
	*/
}

func TestDaemon(t *testing.T) {
	_ = runtime.GOMAXPROCS(1)
	for _, tc := range testCases {
		runTestCase(tc, t)
	}
}

// Runs a single test case. Starts and stops a temp daemon.
func runTestCase(tc daemonTestCase, t *testing.T) {
	//initialize a daemon
	daemon := new(dyconfDaemon)
	go daemon.init("/tmp/qwerty1234")
	/*
	   if err != nil {
	     t.Errorf("Failed to initialize the daemon. Err: [%v]\n", err)
	     return
	   }
	*/
	defer daemon.stop()

	// Run the test case after about a sec
	<-time.After(time.Second * 1)
	finalResp, err := sendRequests(tc.requestSequence)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if tc.finalResponse != finalResp {
		t.Errorf(
			"Final responses don't match!\n"+
				"Request Sequence:\n%v\n"+
				"Expected final response: [%s]\n"+
				"Received final response: [%s]\n"+
				"Expected final response: [%v]\n"+
				"Received final response: [%v]\n"+
				"Length expected: [%d], Length received: [%d]",
			tc.requestSequence,
			tc.finalResponse,
			finalResp,
			[]byte(tc.finalResponse),
			[]byte(finalResp),
			len(tc.finalResponse),
			len(finalResp),
		)
	}
}

func sendRequests(requests []testRequest) (string, error) {
	var finalResp string
	for _, request := range requests {
		resp, err := sendSingleRequest(request)
		if err != nil {
			return string(resp), err
		}
		finalResp = string(resp)
	}
	return strings.TrimSpace(finalResp), nil
}

func sendSingleRequest(request testRequest) ([]byte, error) {
	reqURL, err := url.Parse("http://localhost:8088/")
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set("key", request.key)
	params.Set("value", request.value)

	reqURL.RawQuery = params.Encode()
	reqURL.Path = "/" + request.reqType

	httpReq, err := http.NewRequest("GET", reqURL.String(), nil)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(resp.Body)
}
