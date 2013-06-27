package main

import (
//"github.com/gow/otfc/config"
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
