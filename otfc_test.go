package otfc

import (
	//"fmt"
	"bytes"
	"testing"
	"unsafe"
)

func TestOTFCStructSize(t *testing.T) {
	//expectedSize := uintptr(24 /* header size */ + (24 * INDEX_SIZE) /* 24 bytes X 1023 index records */ + DATA_BLOCK_SIZE /* Size of the config data block */)
	expectedSize := uint32(CONFIG_FILE_SIZE)
	config := Config{}
	actualSize := uint32(unsafe.Sizeof(config))
	if actualSize != expectedSize {
		t.Errorf("Expected size: [%d], Actual size: [%d]", expectedSize, actualSize)
	}
}

func TestOTFCSets(t *testing.T) {
	testKey := "TestOTFCSetsKey"
	testValue := []byte("Some test value with special characters. Tab: [	] CtrlA[]")
	Init("/tmp/qqq1234")
	Set(testKey, testValue)
	retrivedValue, err := Get(testKey)

	if err != nil {
		t.Errorf("Expected no errors; but received [%s]", err)
		return
	}
	if !bytes.Equal(retrivedValue, testValue) {
		t.Errorf("Expected value: [%x], Retrived value [%x]",
			testValue,
			retrivedValue)
	}
}

/*
func ExampleOTCF() {
	config := &OTFC{}
	fmt.Println("Num Records: ", config.NumRecords())
	//Output:
	//Num Records:  0
}

func Example2_OTCF() {
	var config *OTFC
	config.Init()
	fmt.Println("Num Records: ", config.NumRecords())
	//Output:
	//Num Records: 
}
*/
