package otfc

import ()

const (
	ERR_INDEX_FULL              = 01
	ERR_INDEX_KEY_NOT_FOUND     = 02
	ERR_CONFIG_SET_EXISTING_KEY = 03
)

type ConfigError struct {
	errNo   int    // Error Number
	errInfo string // Additional error info
}

func (e ConfigError) Error() string {
	errString := "Unknown Error"
	switch e.errNo {
	case ERR_INDEX_FULL:
		errString = "index block max capacity reached"
	case ERR_INDEX_KEY_NOT_FOUND:
		errString = "index key not found"
	case ERR_CONFIG_SET_EXISTING_KEY:
		errString = "key already exists. Use overwrite() to overwrite it"
	}
	return "Error: " + errString + ". " + e.errInfo
}

func (e ConfigError) ErrNo() int {
	return e.errNo
}
