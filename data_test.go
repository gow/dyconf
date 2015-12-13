package dyconf

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

const (
	recordOverheadBytes = 12
)

func concatBytes(slices ...[]byte) []byte {
	ret := []byte{}
	for _, b := range slices {
		ret = append(ret, b...)
	}
	return ret
}

var headerBytes = make([]byte, 4*sizeOfUint32)

func TestDataBlockFetch(t *testing.T) {

	cases := []struct {
		db            *dataBlock
		startOffset   dataOffset
		key           string
		expectedBytes []byte
	}{
		{ // Case-0: Key is at the head of the list.
			db: &dataBlock{
				block: concatBytes(
					headerBytes,
					[]byte{
						0x07, 0x00, 0x00, 0x00, // key size
						0x09, 0x00, 0x00, 0x00, // data size
						0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
						0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
						0x00, 0x00, 0x00, 0x00, // next (0)
					},
				),
			},
			startOffset:   0x10,
			key:           "TestKey",
			expectedBytes: []byte("TestValue"),
		},
		{ // Case-1: Key is in the middle of the list.
			db: &dataBlock{
				block: concatBytes(
					headerBytes,
					[]byte{ // record-1
						0x04, 0x00, 0x00, 0x00, // key size
						0x04, 0x00, 0x00, 0x00, // data size
						0x44, 0x44, 0x44, 0x44, // key (Junk)
						0x44, 0x44, 0x44, 0x44, // data (Junk)
						0x24, 0x00, 0x00, 0x00, // next (0x20)

						0x07, 0x00, 0x00, 0x00, // key size
						0x09, 0x00, 0x00, 0x00, // data size
						0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
						0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
						0xFF, 0xFF, 0xFF, 0xFF, // next (0xFFFFFFFF). This should not be read.

						0x44, 0x44, 0x44, 0x44, // Junk
						0x44, 0x44, 0x44, 0x44, // Junk
					},
				),
			},
			startOffset:   0x10,
			key:           "TestKey",
			expectedBytes: []byte("TestValue"),
		},
	}

	for i, tc := range cases {
		data, err := tc.db.fetch(tc.startOffset, tc.key)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, data, tc.expectedBytes, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockFetchErrors(t *testing.T) {
	headerBytes := make([]byte, 4*sizeOfUint32)

	cases := []struct {
		db               *dataBlock
		startOffset      dataOffset
		key              string
		expectedErrorStr string
	}{
		{ // Case-0: offset is in header area.
			db: &dataBlock{
				block: []byte{},
			},
			startOffset:      0x08,
			key:              "TestKey",
			expectedErrorStr: "^dataBlock: invalid start offset [0x8]*",
		},
		{ // Case-1: startOffset is out of bound.
			db: &dataBlock{
				block: []byte{},
			},
			startOffset:      0x20,
			key:              "TestKey",
			expectedErrorStr: "^dataBlock: Cannot read out of bound offset [0x20]*",
		},
		{ // Case-2: out of bound access while traversing.
			db: &dataBlock{
				block: concatBytes(
					headerBytes,
					[]byte{
						0x07, 0x00, 0x00, 0x00, // key size
						0x09, 0x00, 0x00, 0x00, // data size
						0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
						0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
						0xFF, 0x00, 0x00, 0x00, // next (0)
					},
				),
			},
			startOffset:      0x10,
			key:              "NonExistingKey",
			expectedErrorStr: `^dataBlock: Cannot read out of bound offset \[0xff\]*`,
		},
		{ // Case-3: Key is not found.
			db: &dataBlock{
				block: concatBytes(
					headerBytes,
					[]byte{
						0x07, 0x00, 0x00, 0x00, // key size
						0x09, 0x00, 0x00, 0x00, // data size
						0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
						0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
						0x00, 0x00, 0x00, 0x00, // next (0)
					},
				),
			},
			startOffset:      0x10,
			key:              "NonExistingKey",
			expectedErrorStr: `^dataBlock: key \[NonExistingKey\] was not found starting at \[10\]*`,
		},
	}

	for i, tc := range cases {
		data, err := tc.db.fetch(tc.startOffset, tc.key)
		ensure.True(t, (data == nil), fmt.Sprintf("Case: [%d]", i))
		//ensure.DeepEqual(t, data, tc.expectedBytes, fmt.Sprintf("Case: [%d]", i))
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrorStr), fmt.Sprintf("Case: [%d]", i))
	}
}

// TestDataBlockSave tests succesful saving of key value pairs in a given data block.
func TestDataBlockSave(t *testing.T) {
	cases := []struct {
		kvPairs       map[string][]byte
		order         []string
		expectedBlock []byte
	}{
		{ // Case-0:
			kvPairs: map[string][]byte{"key": []byte("value")},
			order:   []string{"key"},
			expectedBlock: concatBytes(
				headerBytes,
				[]byte{
					0x03, 0x00, 0x00, 0x00, // key size (3)
					0x05, 0x00, 0x00, 0x00, // data size (5)
					0x6b, 0x65, 0x79, // Key (key)
					0x76, 0x61, 0x6C, 0x75, 0x65, // data (value)
					0x00, 0x00, 0x00, 0x00, // next (0)
				},
			),
		},
		{ // Case-1
			kvPairs: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
			order: []string{"key1", "key3", "key2"},
			expectedBlock: concatBytes(
				headerBytes,
				[]byte{
					0x04, 0x00, 0x00, 0x00, // key size (4)
					0x06, 0x00, 0x00, 0x00, // data size (6)
					0x6b, 0x65, 0x79, 0x31, // key (key1)
					0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, // data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)

					0x04, 0x00, 0x00, 0x00, // key size (4)
					0x06, 0x00, 0x00, 0x00, // data size (6)
					0x6b, 0x65, 0x79, 0x33, // key (key1)
					0x76, 0x61, 0x6C, 0x75, 0x65, 0x33, // data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)

					0x04, 0x00, 0x00, 0x00, // key size (4)
					0x06, 0x00, 0x00, 0x00, // data size (6)
					0x6b, 0x65, 0x79, 0x32, // key (key1)
					0x76, 0x61, 0x6C, 0x75, 0x65, 0x32, // data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)
				},
			),
		},
	}

	for i, tc := range cases {
		buf := make([]byte, len(tc.expectedBlock))
		db := &dataBlock{block: buf, writeOffset: dataBlockHeaderSize}
		for _, key := range tc.order {
			val := tc.kvPairs[key]
			_, err := db.save(key, val)
			ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		}
		ensure.DeepEqual(t, buf, tc.expectedBlock, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockSaveErrors(t *testing.T) {
	cases := []struct {
		keys      []string
		values    [][]byte
		blockSize int
	}{
		{ // empty keys.
			keys:      []string{""},
			values:    [][]byte{[]byte("123")},
			blockSize: 320,
		},
		{ // empty values.
			keys:      []string{"key"},
			values:    [][]byte{[]byte{}},
			blockSize: 320,
		},
		{ // empty keys & values.
			keys:      []string{"key1", "key2", "key3", "key4"},
			values:    [][]byte{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4")},
			blockSize: 60,
		},
	}

	for i, tc := range cases {
		db := &dataBlock{
			writeOffset: dataBlockHeaderSize,
			block:       append(headerBytes, make([]byte, tc.blockSize)...),
		}
		for j, key := range tc.keys {
			_, err := db.save(key, tc.values[j])
			// expect an error only while saving the last record.
			if j == len(tc.keys)-1 {
				ensure.Err(
					t,
					err,
					regexp.MustCompile("(^dataBlock: Cannot write to offset|^Unable to write the key*|^dataBlock save failed*)"),
					fmt.Sprintf("Case: [%d]", i),
				)
			} else {
				// Saving all other keys should not result in error.
				ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
			}
		}
	}
}

// TestDataRecordWrite tests succesfull writes into the given valid block.
func TestDataRecordWrite(t *testing.T) {
	cases := []struct {
		rec      *dataRecord
		expected []byte
	}{
		{ // Case-0: Empty records are 12 bytes.
			rec:      &dataRecord{},
			expected: make([]byte, 12),
		},
		{ // Case-1: Should be able to write data into bigger buffer.
			rec:      &dataRecord{},
			expected: make([]byte, 32),
		},
		{ // Case-2
			rec: &dataRecord{key: []byte("TestKey"), data: []byte("TestValue")},
			expected: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
				0x00, 0x00, 0x00, 0x00, // next (0)
			},
		},
	}

	for i, tc := range cases {
		buf := make([]byte, len(tc.expected))
		err := tc.rec.write(buf)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, buf, tc.expected, fmt.Sprintf("Case: [%d]", i))
	}
}

// TestDataRecordWriteErrors tests errors encountered while writing dataRecord.
func TestDataRecordWriteErrors(t *testing.T) {
	err := (&dataRecord{key: []byte("TEST")}).write(make([]byte, 11))
	ensure.Err(t, err, regexp.MustCompile("Unable to write the key [TEST]*"))
}

// TestDataRecordRead tests succesfull reads into a dataRecord.
func TestDataRecordRead(t *testing.T) {
	cases := []struct {
		block       []byte
		expectedRec *dataRecord
	}{
		{ // Case-0: Empty records.
			block:       make([]byte, 12),
			expectedRec: &dataRecord{key: []byte{}, data: []byte{}},
		},
		{ // Case-1: Reading an empty record from a bigger block of data.
			block:       make([]byte, 32),
			expectedRec: &dataRecord{key: []byte{}, data: []byte{}},
		},
		{ // Case-2: Reading a non-empty record with exact matching bytes.
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
				0x00, 0x00, 0x00, 0x00, // next (0)
			},
			expectedRec: &dataRecord{key: []byte("TestKey"), data: []byte("TestValue")},
		},
		{ // Case-3: Reading a non-empty record from a bigger block of data.
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
				0x04, 0x03, 0x02, 0x01, // next (0)

				0x99, 0x99, 0x99, 0x99, // junk
				0x99, 0x99, 0x99, 0x99,
			},
			expectedRec: &dataRecord{key: []byte("TestKey"), data: []byte("TestValue"), next: 0x01020304},
		},
	}

	for i, tc := range cases {
		rec, err := (&dataRecord{}).read(tc.block)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, rec, tc.expectedRec, fmt.Sprintf("Case: [%d]", i))
	}
}

// TestDataRecordReadErrors tests for various errors in dataRecord.read() method.
func TestDataRecordReadErrors(t *testing.T) {
	cases := []struct {
		block          []byte
		expectedErrStr string
	}{
		{ // Case-0
			block:          []byte{},
			expectedErrStr: "^dataRecord: failed to read the key size*",
		},
		{ // Case-1
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
			},
			expectedErrStr: "^dataRecord: failed to read the data size*",
		},
		{ // Case-2
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
			},
			expectedErrStr: "^dataRecord: failed to read the key*",
		},
		{ // Case-3
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
			},
			expectedErrStr: "^dataRecord: failed to read the data*",
		},
		{ // Case-4
			block: []byte{
				0x07, 0x00, 0x00, 0x00, // key size
				0x09, 0x00, 0x00, 0x00, // data size
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
			},
			expectedErrStr: "^dataRecord: failed to read the next pointer*",
		},
	}

	for i, tc := range cases {
		_, err := (&dataRecord{}).read(tc.block)
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("Case: [%d]", i))
	}
}
