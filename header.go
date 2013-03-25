package otfc

type headerBlock struct {
	version    uint16
	lock       uint16
	totalSize  uint32
	updateTime uint64
	numRecords uint32
	padding    [4]byte
}
