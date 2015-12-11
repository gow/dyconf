package dyconf

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

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

func TestDataRecordWriteErrors(t *testing.T) {
	err := (&dataRecord{key: []byte("TEST")}).write(make([]byte, 11))
	ensure.Err(t, err, regexp.MustCompile("Unable to write the key [TEST]*"))
}
