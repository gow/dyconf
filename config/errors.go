package config

const (
	ERR_INDEX_FULL              = 2001
	ERR_INDEX_KEY_NOT_FOUND     = 2002
	ERR_INDEX_INACTIVE          = 2003
	ERR_CONFIG_SET_EXISTING_KEY = 1004
)

type ErrorIface interface {
  GetErrorNo() int
  GetErrorString() string
  //MarshalJSON() ([]byte, error)
  Error() string
}

type Error struct {
	ErrNo   int    // Error Number
	ErrInfo string // Additional error info
}

func (e Error) Error() string {
	return "Error: " + e.ErrorString()
}

func (e Error) GetErrorNo() int {
  return e.ErrNo
}
func (e Error) GetErrorString() string {
  return e.ErrorString()
}

func (e Error) ErrorString() string {
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
	return errString + ". " + e.ErrInfo
}
