package config

const (
	ERR_INDEX_FULL              = 01
	ERR_INDEX_KEY_NOT_FOUND     = 02
	ERR_INDEX_INACTIVE          = 03
	ERR_CONFIG_SET_EXISTING_KEY = 04
)

type Error struct {
	ErrNo   int    // Error Number
	ErrInfo string // Additional error info
}

func (e Error) Error() string {
	errString := "Unknown Error"
	switch e.ErrNo {
	case ERR_INDEX_FULL:
		errString = "index block has reached max capacity"
	case ERR_INDEX_KEY_NOT_FOUND:
		errString = "index key not found"
	case ERR_CONFIG_SET_EXISTING_KEY:
		errString = "key already exists. Use overwrite() to overwrite it"
	case ERR_INDEX_INACTIVE:
		errString = "key is either inactive or deleted"
	}
	return "Error: " + errString + ". " + e.ErrInfo
}
