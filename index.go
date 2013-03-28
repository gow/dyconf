package otfc

// Represents an index record. The offset points to the actual config data in the data block
type indexRecord struct {
	key     [16]byte //md5 hash of the config key
	offset  uint32
	status  byte
	padding [3]byte
}
