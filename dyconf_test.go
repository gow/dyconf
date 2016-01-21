package dyconf

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestDyconfSetGetClose(t *testing.T) {
	cases := []struct {
		data map[string][]byte
	}{
		{
			data: map[string][]byte{
				"Key1": []byte("Value1"),
				"Key2": []byte("Value2"),
				"Key3": []byte("Value3"),
			},
		},
	}

	for i, tc := range cases {
		// Setup
		tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfSetGetClose-Case%d-", i))
		defer os.Remove(tmpFileName)

		// Set the keys.
		wc, err := NewManager(tmpFileName)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		for key, val := range tc.data {
			err = wc.Set(key, val)
			ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		}

		// Get the keys.
		conf, err := New(tmpFileName)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		for key, expectedVal := range tc.data {
			val, err := conf.Get(key)
			ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
			ensure.DeepEqual(t, val, expectedVal, fmt.Sprintf("Case: [%d]", i))
		}

		// Close the writer.
		err = wc.Close()
		ensure.Nil(t, err, fmt.Sprintf("Case-%d", i))

		// Close the reader.
		err = conf.Close()
		ensure.Nil(t, err, fmt.Sprintf("Case-%d", i))
	}
}

func TestDyconfOverwrite(t *testing.T) {
	setSequence := []struct {
		key string
		val []byte
	}{
		{key: "overwrite key", val: []byte("big value 1")},
		{key: "overwrite key", val: []byte("Bigger Value 1")},
		{key: "overwrite key", val: []byte("Bigger Value 2")},
		{key: "some other key", val: []byte("some other value")},
		{key: "overwrite key", val: []byte("very big value 1")},
		{key: "overwrite key", val: []byte("smallval")},
	}
	expected := map[string][]byte{
		"overwrite key":  []byte("smallval"),
		"some other key": []byte("some other value"),
	}

	// Setup
	tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfOverwrite-"))
	defer os.Remove(tmpFileName)

	// Set the keys in the given sequence.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	for _, kvPair := range setSequence {
		err = wc.Set(kvPair.key, kvPair.val)
		ensure.Nil(t, err)
	}

	// Check if the results are as expected.
	conf, err := New(tmpFileName)
	ensure.Nil(t, err)
	for key, expectedVal := range expected {
		val, err := conf.Get(key)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val, expectedVal)
	}

	err = conf.Close()
	ensure.Nil(t, err)
	err = wc.Close()
	ensure.Nil(t, err)
}

func TestDyconfCollisions(t *testing.T) {
	setSequence := []struct {
		key string
		val []byte
	}{
		{key: "key-1", val: []byte("big value 1")},
		{key: "key-1", val: []byte("Bigger Value 1")},
		{key: "key-1", val: []byte("Bigger Value 2")},
		{key: "some other key", val: []byte("some other value")},
		{key: "key-1", val: []byte("very big value 1")},
		{key: "key-2", val: []byte("Value-222")},
		{key: "key-1", val: []byte("smallval")},
		{key: "key-2", val: []byte("Value-2")},
	}
	expected := map[string][]byte{
		"key-1":          []byte("smallval"),
		"key-2":          []byte("Value-2"),
		"some other key": []byte("some other value"),
	}

	// Setup
	tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfCollisions-"))
	defer os.Remove(tmpFileName)

	// replace hashing function.
	savedHashfunc := defaultHashFunc
	defaultHashFunc = func(key string) (uint32, error) {
		return 32, nil // Everything falls into bucket-32
	}
	defer func() {
		defaultHashFunc = savedHashfunc // restore
	}()

	// Set the keys in the given sequence.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	for _, kvPair := range setSequence {
		err = wc.Set(kvPair.key, kvPair.val)
		ensure.Nil(t, err)
	}

	// Check if the results are as expected.
	conf, err := New(tmpFileName)
	ensure.Nil(t, err)
	for key, expectedVal := range expected {
		val, err := conf.Get(key)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val, expectedVal)
	}

	err = conf.Close()
	ensure.Nil(t, err)
	err = wc.Close()
	ensure.Nil(t, err)
}

func TestDyconfDelete(t *testing.T) {
	setSequence := []struct {
		key string
		val []byte
	}{
		{key: "key-1", val: []byte("big value 1")},
		{key: "key-1", val: []byte("Bigger Value 1")},
		{key: "key-1", val: []byte("Bigger Value 2")},
		{key: "Non deleted key", val: []byte("Non deleted value")},
		{key: "key-1", val: []byte("very big value 1")},
		{key: "key-2", val: []byte("Value-222")},
		{key: "key-3", val: []byte("Value-3")},
		{key: "key-1", val: []byte("smallval")},
		{key: "key-2", val: []byte("Value-2")},
		{key: "One More Non deleted key", val: []byte("One more Non deleted value")},
	}
	deleteKeys := []string{"key-1", "key-2", "key-3", "NonExistingKey"}
	expected := map[string][]byte{
		"Non deleted key":          []byte("Non deleted value"),
		"One More Non deleted key": []byte("One more Non deleted value"),
	}

	// Setup
	tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfDelete-"))
	defer os.Remove(tmpFileName)

	// Set the keys in the given sequence.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	for _, kvPair := range setSequence {
		err = wc.Set(kvPair.key, kvPair.val)
		ensure.Nil(t, err)
	}
	// delete the keys.
	for _, delKey := range deleteKeys {
		err = wc.Delete(delKey)
		ensure.Nil(t, err)
	}

	// deleted keys must be gone.
	conf, err := New(tmpFileName)
	ensure.Nil(t, err)
	for _, delKey := range deleteKeys {
		val, err := conf.Get(delKey)
		ensure.Err(t, err, regexp.MustCompile(`^dyconf: key .* was not found.*`))
		ensure.True(t, (val == nil))
	}

	// expected key-value pairs must be present.
	for key, expectedVal := range expected {
		val, err := conf.Get(key)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val, expectedVal)
	}

	// Close.
	err = conf.Close()
	ensure.Nil(t, err)
	err = wc.Close()
	ensure.Nil(t, err)
}

func TestDyconfDeleteWithCollisions(t *testing.T) {
	setSequence := []struct {
		key string
		val []byte
	}{
		{key: "key-1", val: []byte("big value 1")},
		{key: "key-1", val: []byte("Bigger Value 1")},
		{key: "key-1", val: []byte("Bigger Value 2")},
		{key: "Non deleted key", val: []byte("Non deleted value")},
		{key: "key-1", val: []byte("very big value 1")},
		{key: "key-2", val: []byte("Value-222")},
		{key: "key-3", val: []byte("Value-3")},
		{key: "key-1", val: []byte("smallval")},
		{key: "key-2", val: []byte("Value-2")},
		{key: "One More Non deleted key", val: []byte("One more Non deleted value")},
	}
	deleteKeys := []string{"key-1", "key-2", "key-3", "NonExistingKey"}
	expected := map[string][]byte{
		"Non deleted key":          []byte("Non deleted value"),
		"One More Non deleted key": []byte("One more Non deleted value"),
	}

	// Setup
	tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfDeleteWithCollisions-"))
	defer os.Remove(tmpFileName)

	// replace hashing function.
	savedHashfunc := defaultHashFunc
	defaultHashFunc = func(key string) (uint32, error) {
		i, err := savedHashfunc(key)
		return i % 2, err // Everything falls into either bucket 0 or 1
	}
	defer func() {
		defaultHashFunc = savedHashfunc // restore
	}()

	// Set the keys in the given sequence.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	for _, kvPair := range setSequence {
		err = wc.Set(kvPair.key, kvPair.val)
		ensure.Nil(t, err)
	}
	// delete the keys.
	for _, delKey := range deleteKeys {
		err = wc.Delete(delKey)
		ensure.Nil(t, err)
	}

	// deleted keys must be gone.
	conf, err := New(tmpFileName)
	ensure.Nil(t, err)
	for _, delKey := range deleteKeys {
		val, err := conf.Get(delKey)
		ensure.Err(t, err, regexp.MustCompile(`^dyconf: key .* was not found.*`))
		ensure.True(t, (val == nil))
	}

	// expected key-value pairs must be present.
	for key, expectedVal := range expected {
		val, err := conf.Get(key)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val, expectedVal)
	}

	// Close.
	err = conf.Close()
	ensure.Nil(t, err)
	err = wc.Close()
	ensure.Nil(t, err)
}

func TestDyconfInitErrors(t *testing.T) {
	// try opening a non existing file.
	conf, err := New("/tmp/dyconf-nonexisting-rkkbnbrejhhfellgkrhleuhutncdejvr")
	ensure.Err(t, err, regexp.MustCompile(`^dyconf: failed to open the file.*`))
	ensure.Nil(t, conf)
}

func TestDyconfWriteInitNewFile(t *testing.T) {
	// Create the file first.
	tmpFileName := setupTempFile(t, "TestDyconfWriteInitNewFile-")
	os.Remove(tmpFileName)

	// Initialize the writer.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	ensure.Nil(t, wc.Close())

	// Make sure the file is created and then delete it.
	_, err = os.Stat(tmpFileName)
	ensure.Nil(t, err)
	ensure.Nil(t, os.Remove(tmpFileName))
}

// TestDyconfWriteInitExistingFile tests the initialization of existing config file for writing.
func TestDyconfWriteInitExistingFile(t *testing.T) {
	// Create the file first.
	tmpFileName := setupTempFile(t, "TestDyconfWriteInitExistingFile-")
	os.Remove(tmpFileName)

	// Initialize the writer and create a new config file.
	m, err := NewManager(tmpFileName)
	ensure.Nil(t, err)
	ensure.Nil(t, m.Close())
	// Make sure the file is created.
	_, err = os.Stat(tmpFileName)
	ensure.Nil(t, err)

	// Initialize the writer with the existing config file.
	m, err = NewManager(tmpFileName)
	ensure.Nil(t, err)
	ensure.Nil(t, m.Close())
}

func setupTempFile(t *testing.T, prefix string) string {
	tmpFile, err := ioutil.TempFile("", prefix)
	ensure.Nil(t, err)
	defer tmpFile.Close()
	os.Remove(tmpFile.Name())

	return tmpFile.Name()
}

func TestDyconfMap(t *testing.T) {
	cases := []struct {
		kv []struct {
			key string
			val []byte
		}
	}{
		{ // Case-0: key1 is overwritten.
			kv: []struct {
				key string
				val []byte
			}{
				{key: "key1", val: []byte("val1")},
				{key: "key2", val: []byte("val2")},
				{key: "key1", val: []byte("val1_1")},
				{key: "key3", val: []byte("val3")},
			},
		},
	}

	// Test with normal hashing.
	for i, tc := range cases {
		tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfMap-case-%d-", i))
		// Initialize the writer.
		wc, err := NewManager(tmpFileName)
		defer os.Remove(tmpFileName)
		ensure.Nil(t, err)

		expected := make(map[string][]byte)
		for _, kv := range tc.kv {
			ensure.Nil(t, wc.Set(kv.key, kv.val))
			expected[kv.key] = kv.val
		}
		retMap, err := wc.Map()
		ensure.Nil(t, err, fmt.Sprintf("Case-%d", i))
		ensure.DeepEqual(t, retMap, expected, fmt.Sprintf("Case-%d", i))
		ensure.Nil(t, wc.Close())
	}

	// Test with collisions.
	savedHashfunc := defaultHashFunc
	defaultHashFunc = func(key string) (uint32, error) {
		return 20, nil // Everything falls into bucket-20
	}
	defer func() {
		defaultHashFunc = savedHashfunc // restore
	}()
	for i, tc := range cases {
		tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfMap-case-%d-", i))
		// Initialize the writer.
		wc, err := NewManager(tmpFileName)
		defer os.Remove(tmpFileName)
		ensure.Nil(t, err)

		expected := make(map[string][]byte)
		for _, kv := range tc.kv {
			ensure.Nil(t, wc.Set(kv.key, kv.val))
			expected[kv.key] = kv.val
		}
		retMap, err := wc.Map()
		ensure.Nil(t, err, fmt.Sprintf("Case-%d", i))
		ensure.DeepEqual(t, retMap, expected, fmt.Sprintf("Case-%d", i))
		ensure.Nil(t, wc.Close())
	}
}

func TestDyconfDefrag(t *testing.T) {
	kvPairs := []struct {
		key string
		val []byte
	}{
		// overwrite the same key with different data size causing fragmentation.
		{key: "key", val: []byte("val1")},
		{key: "key", val: []byte("val22")},
		{key: "key", val: []byte("val333")},
		{key: "key", val: []byte("val4444")},
	}
	expectedUsedByteCount := (&dataRecord{key: []byte("key"), data: []byte("val4444")}).size()

	// Initialize the writer.
	tmpFileName := setupTempFile(t, fmt.Sprintf("TestDyconfDefrag-"))
	m, err := NewManager(tmpFileName)
	defer os.Remove(tmpFileName)
	ensure.Nil(t, err)

	for _, kv := range kvPairs {
		ensure.Nil(t, m.Set(kv.key, kv.val))
	}
	// save previous free byte count
	prevFreeBytes, err := m.freeDataByteCount()
	ensure.Nil(t, err)

	// defrag
	ensure.Nil(t, m.Defrag())

	// new free byte count
	newFreeBytes, err := m.freeDataByteCount()
	ensure.Nil(t, err)

	// new free byte count must be greater than previous one.
	ensure.True(t, newFreeBytes > prevFreeBytes)

	// Also verify the size of the data block.
	usedByteCount, err := m.dataBlockSize()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, usedByteCount, expectedUsedByteCount)

	ensure.Nil(t, m.Close())
}
