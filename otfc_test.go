package otfc

import (
	"os"
	//"fmt"
	"bytes"
	"testing"
	"time"
	"unsafe"
	//"crypto/md5"
	"log"
	"math/rand"
)

func TestOTFCStructSize(t *testing.T) {
	expectedSize := uint32(CONFIG_FILE_SIZE)
	config := Config{}
	actualSize := uint32(unsafe.Sizeof(config))
	if actualSize != expectedSize {
		t.Errorf("Expected size: [%d], Actual size: [%d]", expectedSize, actualSize)
	}
}

// Tests a single Set() and Get()
func TestOTFCSingleSetAndGet(t *testing.T) {
	confFile := getTempFileName()
	defer os.Remove(confFile)

	testKey := "TestOTFCSetsKey"
	testValue := []byte("Some test value with special characters. Tab: [	] CtrlA[]")
	Init(confFile)
	defer Shutdown()
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

// Tests multiple Set()s sequentially
func TestOTFCSequentialMultipleSets(t *testing.T) {
	MAX_KEY_SIZE := 128  //chars
	MAX_VALUE_SIZE := 50 //bytes
	confFile := getTempFileName()
	defer os.Remove(confFile)

	seedVal := time.Now().Unix()
	rand.Seed(seedVal)
	randomLimit := rand.Intn(MAX_INDEX_RECORDS)
	log.Printf("Testing multiple sets with %d samples (seed: %d)\n", randomLimit, seedVal)

	inputMap := map[string][]byte{}
	Init(confFile)
	defer Shutdown()
	for len(inputMap) < randomLimit {
		key := getRandomLengthString(MAX_KEY_SIZE)
		val := getRandomLengthByteSlice(MAX_VALUE_SIZE)
		if _, ok := inputMap[key]; ok {
			continue // Key already exists
		}
		inputMap[key] = val
		if err := Set(key, val); err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%s]", err)
			return
		}
	}
}

// Tests multiple Set()s and Get()s sequentially
func TestOTFCSequentialMultipleSetsGetsDeletes(t *testing.T) {
	MAX_KEY_SIZE := 128  //chars
	MAX_VALUE_SIZE := 50 //bytes
	confFile := getTempFileName()
	defer os.Remove(confFile)

	seedVal := int64(32) //Let the generated {key, values} be deterministic
	rand.Seed(seedVal)
	randomLimit := rand.Intn(MAX_INDEX_RECORDS)
	log.Printf("Testing multiple sets & gets with %d samples (seed: %d)\n", randomLimit, seedVal)
	Init(confFile)
	defer Shutdown()
	inputMap := map[string][]byte{}
	for len(inputMap) < randomLimit {
		key := getRandomLengthString(MAX_KEY_SIZE)
		val := getRandomLengthByteSlice(MAX_VALUE_SIZE)
		if _, ok := inputMap[key]; ok {
			continue // Key already exists
		}
		inputMap[key] = val
		if err := Set(key, val); err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%s]", err)
			return
		}
	}
	for key, val := range inputMap {
		retrivedValue, err := Get(key)

		if err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%v]", err)
			return
		}
		if !bytes.Equal(retrivedValue, val) {
			t.Errorf("Incorrect value retrived. Key: [%s], Expected value: [%x], Retrived value [%x]",
				key,
				val,
				retrivedValue)
			return
		}
	}

	for key, _ := range inputMap {
		err := Delete(key)
		if err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%v]", err)
			return
		}
	}
}

// Tests a Get() on an empty config file
func TestOTFCEmptyGet(t *testing.T) {
	confFile := getTempFileName()
	defer os.Remove(confFile)

	testKey := "SomeNonExistant key"
	Init(confFile)
	defer Shutdown()
	retrivedValue, err := Get(testKey)
	if err == nil {
		t.Errorf("Expected error; but none received")
	} else {
		expectConfigError(t, ERR_INDEX_KEY_NOT_FOUND, err)
	}
	if retrivedValue != nil {
		t.Errorf("Value received for non-existant key. key: [%s], value [%x]",
			testKey,
			retrivedValue)
	}
	return
}

// Tests double sets of the same key.
func TestOTFCDoubleSets(t *testing.T) {
	confFile := getTempFileName()
	defer os.Remove(confFile)

	testKey := "KeyThatWillBeSetTwice"
	randomValue1 := []byte(getRandomString(64))
	randomValue2 := []byte(getRandomString(64))
	Init(confFile)
	defer Shutdown()
	if err := Set(testKey, randomValue1); err != nil {
		t.Errorf("Expected no errors; but received [%s]", err)
		return
	}
	err := Set(testKey, randomValue2)
	if err == nil {
		t.Errorf("Expected an error; but none received")
	} else {
		expectConfigError(t, ERR_CONFIG_SET_EXISTING_KEY, err)
	}
	return
}

// Test Max index capacity
func TestOTFCMaxIndexCapacity(t *testing.T) {
	MAX_KEY_SIZE := 128 //chars
	MAX_VALUE_SIZE := 5 //bytes. Keeping it small to avoid filling the data block.
	confFile := getTempFileName()
	defer os.Remove(confFile)

	rand.Seed(32) //Let the generated {key, values} be deterministic

	Init(confFile)
	defer Shutdown()
	inputMap := map[string][]byte{}
	// Fille the config.
	for len(inputMap) < MAX_INDEX_RECORDS {
		key := getRandomLengthString(MAX_KEY_SIZE)
		val := getRandomLengthByteSlice(MAX_VALUE_SIZE)
		if _, ok := inputMap[key]; ok {
			continue
		}
		inputMap[key] = val
		if err := Set(key, val); err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%s]", err)
			return
		}
	}

	//log.Printf("Config should contain %d elements", len(inputMap))
	overflowAttempts := 5
	for overflowAttempts > 0 {
		key := getRandomLengthString(MAX_KEY_SIZE)
		val := getRandomLengthByteSlice(MAX_VALUE_SIZE)
		if _, ok := inputMap[key]; ok {
			continue
		}

		overflowAttempts--
		err := Set(key, val)
		if err == nil {
			t.Errorf("Expected error; but none received")
		} else {
			expectConfigError(t, ERR_INDEX_FULL, err)
		}
	}
}

// Tests a delete call on an empty config file.
func TestOTFCEmptyDelete(t *testing.T) {
	log.Printf("Testing Empty delete\n")
	randomize()
	MAX_KEY_SIZE := 256 //chars
	//MAX_VALUE_SIZE := 50 //bytes
	confFile := getTempFileName()
	defer os.Remove(confFile)

	key := getRandomLengthString(MAX_KEY_SIZE)

	Init(confFile)
	defer Shutdown()

	err := Delete(key)
	if err != nil {
		Print()
		t.Errorf("Expected no errors; but received [%s]", err)
		return
	}
}

// Tests the deletion on non existing keys
func TestOTFCDeleteNonExistingKey(t *testing.T) {
	log.Println("Testing deletion of non existing keys")
	randomize()
	MAX_KEY_SIZE := 256 //chars
	MAX_VALUE_SIZE := 64
	randomLimit := rand.Intn(MAX_INDEX_RECORDS)

	Init(getTempFileName())
	defer Shutdown()
	inputMap := map[string][]byte{}
	inputMap["test"] = []byte("Value")
	Set("test", []byte("value"))
	for len(inputMap) < randomLimit {
		key := getRandomLengthString(MAX_KEY_SIZE)
		val := getRandomLengthByteSlice(MAX_VALUE_SIZE)
		if _, ok := inputMap[key]; ok {
			continue
		}
		inputMap[key] = val
		err := Delete(key)
		expectConfigError(t, ERR_INDEX_KEY_NOT_FOUND, err)
		if err := Set(key, val); err != nil {
			Print()
			t.Errorf("Expected no errors; but received [%s]", err)
			return
		}
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
  defer Shutdown()
	fmt.Println("Num Records: ", config.NumRecords())
	//Output:
	//Num Records: 
}
*/

/////////// Helper functions /////////////
func randomize() {
	seedVal := time.Now().Unix()
	rand.Seed(seedVal)
	log.Printf("Random seed value: [%d]", seedVal)
}
func expectConfigError(t *testing.T, errNo int, err error) bool {
	if configError, ok := err.(ConfigError); ok {
		if configError.ErrNo() == errNo {
			return true
		}
	}
	t.Errorf("Expected error [%s]; received [%s]", ConfigError{errNo, ""}, err)
	return false
}

func getTempFileName() string {
	return "/tmp/otfc_test_" + getRandomString(8)
}

func getRandomLengthString(maxLength int) string {
	return getRandomString(rand.Intn(maxLength-1) + 1)
}
func getRandomString(length int) string {
	return string(getRandomByteSlice(length))
}

func getRandomLengthByteSlice(maxLength int) []byte {
	return getRandomByteSlice(rand.Intn(maxLength-1) + 1)
}
func getRandomByteSlice(length int) []byte {
	var alpha = " _abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	byteSlice := make([]byte, length)
	for i := 0; i < length; i++ {
		byteSlice[i] = alpha[rand.Intn(len(alpha))]
	}
	return byteSlice
}
