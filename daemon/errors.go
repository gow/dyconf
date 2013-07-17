package main

const (
	ERR_DMN_INVALID_KEY   = 6001
	ERR_DMN_INVALID_VALUE = 6002
)

type Error struct {
	ErrNo   int    // Error Number
	ErrInfo string // Additional error info
}

func (e Error) Error() string {
	return "Error: " + e.GetErrorString()
}

func (e Error) GetErrorString() string {
	errString := "Unknown Error"
	switch e.ErrNo {
	case ERR_DMN_INVALID_KEY:
		errString = "Invalid key"
	case ERR_DMN_INVALID_VALUE:
		errString = "Invalid value"
	}
	return errString + ". " + e.ErrInfo
}

func (e Error) GetErrorNo() int {
	return e.ErrNo
}
