package otfc

type configHeader struct {
	version    uint16
	lock       uint16
	totalSize  uint32
	updateTime uint64
	numRecords uint32
	padding    [4]byte
}

func (h *configHeader) Version() uint16 {
	return h.version
}
func (h *configHeader) SetVersion(ver uint16) *configHeader {
	h.version = ver
	return h
}

func (h *configHeader) ByteSize() uint32 {
	return h.totalSize
}

func (h *configHeader) UpdateTime() uint64 {
	return h.updateTime
}

func (h *configHeader) NumRecords() uint32 {
	return h.numRecords
}
