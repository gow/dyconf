package otfc

const (
	INDEX_SIZE      = 1023
	DATA_BLOCK_SIZE = 256 * 1024 // 256k bytes
)

type OTFC struct {
	header headerBlock
	index  [INDEX_SIZE]indexRecord
	data   [DATA_BLOCK_SIZE]byte
}
