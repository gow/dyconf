package config

import (
	"fmt"
	"time"
)

const (
	CONFIG_HEADER_SIZE = 32
)

type configHeader struct {
	version      uint16
	lock         uint16
	totalSize    uint32
	modifiedTime int64
	numRecords   uint32
	writeOffset  uint32
	padding      [8]byte
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

func (h *configHeader) ModifiedTime() time.Time {
	return time.Unix(h.modifiedTime, 0)
}

func (h *configHeader) NumRecords() uint32 {
	return h.numRecords
}

func (h *configHeader) SetRecordCount(count uint32) {
	h.numRecords = count
	h.UpdateTime()
}

func (h *configHeader) UpdateTime() {
	h.modifiedTime = time.Now().Unix()
}

func (h configHeader) print() {
	fmt.Printf("version : %d\n", h.version)
	fmt.Printf("lock : %d\n", h.lock)
	fmt.Printf("totalSize: %d\n", h.totalSize)
	fmt.Printf("modifiedTime : %v\n", h.ModifiedTime())
	fmt.Printf("numRecords : %d\n", h.numRecords)
	fmt.Printf("writeOffset: %d\n", h.writeOffset)
}
