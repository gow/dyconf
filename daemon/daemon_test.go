package main

import (
	"fmt"
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

// Each test case contains a sequence of requests and the expected response for
// the final request.
var testCases = []daemonTestCase{
	// Empty get
	{
		[]testRequest{{"get", "123", ""}},
		`{"Status":"error","Err":{"ErrNo":2002,"ErrMsg":"index key not found. key [123]"}}`,
	},
	// Set
	{
		[]testRequest{{"set", "qqq", "QWErty~~!!"}},
		`{"Status":"OK","Key":"qqq","Value":"QWErty~~!!"}`,
	},
	// Empty Delete
	{
		[]testRequest{{"delete", "test_key", ""}},
		`{"Status":"error","Err":{"ErrNo":2002,"ErrMsg":"index key not found. key [test_key]"}}`,
	},
	// Set & Get
	{
		[]testRequest{
			{"set", "test_key", "test_value"},
			{"get", "test_key", ""},
		},
		`{"Status":"OK","Key":"test_key","Value":"dGVzdF92YWx1ZQ=="}`,
	},
	// Overwrite
	{
		[]testRequest{
			{"set", "test_key", "test_value"},
			{"set", "test_key", "test_value"},
		},
		`{"ErrNo":1004,"ErrMsg":"key already exists. Use overwrite() to overwrite it. key [test_key]"}`,
	},
}

func TestDaemon(t *testing.T) {
	_ = runtime.GOMAXPROCS(1)
	for _, tc := range testCases {
		runTestCase(tc, t)
	}
}

// Runs a single test case. Starts and stops a temp daemon.
func runTestCase(tc daemonTestCase, t *testing.T) {
	fmt.Printf("Running test case: %v\n", tc)
	//initialize a daemon
	daemon := new(dyconfDaemon)
	go daemon.init("/tmp/qwerty1234")
	defer daemon.stop()
	// We need to re-initialize the DefaultServeMux before next testcase.
	// This is to avoid the ["panic: http: multiple registrations for /<path>"]
	// errors caused by the new daemon server trying to register handles that
	// were registered by the daemons server in previous test case.
	defer func() { http.DefaultServeMux = http.NewServeMux() }()

	// Give enough time for the daemon to initialize. Unfortunately, this also
	// means that each testcase will take a minimum of this much time.
	<-time.After(time.Millisecond * 500)

	// Run the test case
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
				"Length expected: [%d]\n"+
				"Length received: [%d]\n"+
				"Expected final response (bytes): [%v]\n"+
				"Received final response (bytes): [%v]\n",
			tc.requestSequence,
			tc.finalResponse,
			finalResp,
			len(tc.finalResponse),
			len(finalResp),
			[]byte(tc.finalResponse),
			[]byte(finalResp),
		)
	}
}

// Sends a sequence of requests and returns the response from the last request.
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

// Sends a single request and returns the body of the response.
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
