package dyconf

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

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
		rec := &dataRecord{}
		err := rec.read(tc.block)
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
		err := (&dataRecord{}).read(tc.block)
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("Case: [%d]", i))
	}
}
