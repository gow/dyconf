package dyconf

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestDyconfSetAndGet(t *testing.T) {
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
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("dyconf-TestGet-Case-%d", i))
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		tmpFileName := tmpFile.Name()
		tmpFile.Close()

		// Set the keys.
		wc, err := NewWriter(tmpFileName)
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

	tmpFile, err := ioutil.TempFile("", "dyconf-TestDyconfOverwrite")
	ensure.Nil(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Set the keys in the given sequence.
	wc, err := NewWriter(tmpFileName)
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
	tmpFile, err := ioutil.TempFile("", "dyconf-TestDyconfCollisions")
	ensure.Nil(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// replace hashing function.
	savedHashfunc := defaultHashFunc
	defaultHashFunc = func(key string) (uint32, error) {
		return 32, nil // Everything falls into bucket-32
	}
	defer func() {
		defaultHashFunc = savedHashfunc // restore
	}()

	// Set the keys in the given sequence.
	wc, err := NewWriter(tmpFileName)
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
	deleteKeys := []string{"key-1", "key-2", "key-3"}
	expected := map[string][]byte{
		"Non deleted key":          []byte("Non deleted value"),
		"One More Non deleted key": []byte("One more Non deleted value"),
	}

	// Setup
	tmpFile, err := ioutil.TempFile("", "dyconf-TestDyconfCollisions")
	ensure.Nil(t, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()

	// Set the keys in the given sequence.
	wc, err := NewWriter(tmpFileName)
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
		ensure.Nil(t, val)
		ensure.Err(t, err, regexp.MustCompile("qwerty"))
	}

	// expected key-value pairs must be present.
	for key, expectedVal := range expected {
		val, err := conf.Get(key)
		ensure.Nil(t, err)
		ensure.DeepEqual(t, val, expectedVal)
	}
}
