package otfc

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestOTFCStructSize(t *testing.T) {
	expectedSize := uintptr(24 /* header size */ + (24 * INDEX_SIZE) /* 24 bytes X 1023 index records */ + DATA_BLOCK_SIZE /* Size of the config data block */)
	config := OTFC{}
	actualSize := unsafe.Sizeof(config)
	if actualSize != expectedSize {
		t.Errorf("Expected size: [%d], Actual size: [%d]", expectedSize, actualSize)
	}
}

func ExampleOTCF() {
	config := &OTFC{}
	fmt.Println("Num Records: ", config.NumRecords())
	//Output:
	//Num Records:  0
}
