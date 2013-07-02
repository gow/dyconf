package main

import (
	"encoding/json"
)

const (
	ERR_DMN_INVALID_VALUE = 6001
)

type Error struct {
	ErrNo   int    // Error Number
	ErrInfo string // Additional error info
}

func (e Error) ErrorString() string {
	errString := "Unknown Error"
	switch e.ErrNo {
	case ERR_DMN_INVALID_VALUE:
		return "Invalid value"
	}
	return errString + ". " + e.ErrInfo
}

func (err *Error) MarshalJSON() ([]byte, error) {
	val := struct {
		ErrNo  int
		ErrMsg string
	}{
		err.ErrNo,
		err.ErrorString(),
	}
	return json.Marshal(val)
}
