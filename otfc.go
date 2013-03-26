package otfc

const (
	INDEX_SIZE      = 1023
	DATA_BLOCK_SIZE = 256 * 1024 // 256k bytes
	CONFIG_FILE     = "/tmp/71ebdf319f2a7fa1d4eb45f9c4b7cf64"
)

type OTFC struct {
	header configHeader
	index  [INDEX_SIZE]indexRecord
	data   [DATA_BLOCK_SIZE]byte
}

func (config *OTFC) NumRecords() uint32 {
	return config.header.NumRecords()
}

func (config *OTFC) Init() {
	// TODO: mmap the config file
}
