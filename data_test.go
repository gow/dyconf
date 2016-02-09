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

func headerBytes(b ...byte) []byte {
	l := uint32(len(b))
	b = append(b, make([]byte, dataBlockHeaderSize-l)...)
	return b
}

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
					headerBytes(),
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
					headerBytes(),
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
		data, found, err := tc.db.fetch(tc.startOffset, tc.key)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		ensure.True(t, found, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, data, tc.expectedBytes, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockFetchErrors(t *testing.T) {
	cases := []struct {
		db             *dataBlock
		startOffset    dataOffset
		key            string
		expectedErrStr string
	}{
		{ // Case-0: offset is in header area.
			db: &dataBlock{
				block: []byte{},
			},
			startOffset:    0x08,
			key:            "TestKey",
			expectedErrStr: "^dataBlock: invalid start offset [0x8]*",
		},
		{ // Case-1: startOffset is out of bound.
			db: &dataBlock{
				block: []byte{},
			},
			startOffset:    0x20,
			key:            "TestKey",
			expectedErrStr: "^dataBlock: Cannot read out of bound offset [0x20]*",
		},
		{ // Case-2: out of bound access while traversing.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(),
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (2), data size (2)
						0x41, 0x41, 0x31, 0x31, 0xFF, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0xFF)
					},
				),
			},
			startOffset:    0x10,
			key:            "NonExistingKey",
			expectedErrStr: `^dataBlock: Cannot read out of bound offset \[0xff\]*`,
		},
		{ // Case-3: key size exceeds max key size
			db: &dataBlock{
				block: concatBytes(
					headerBytes(),
					[]byte{
						0x01, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (0x10001), data size (2)
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x00)
					},
				),
			},
			startOffset:    0x10,
			key:            "NonExistingKey",
			expectedErrStr: `^dataRecord: failed to read the key \(size=0x10001\). It exceeds max size \[0x10000\]*`,
		},
		{ // Case-4: data size exceeds max data size
			db: &dataBlock{
				block: concatBytes(
					headerBytes(),
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x08, // key size (0x10001), data size (0x0800000)
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x00)
					},
				),
			},
			startOffset:    0x10,
			key:            "NonExistingKey",
			expectedErrStr: `^dataRecord: failed to read the data \(size=0x8000001\). It exceeds max size \[0x8000000\]`,
		},
	}

	for i, tc := range cases {
		data, found, err := tc.db.fetch(tc.startOffset, tc.key)
		ensure.True(t, (data == nil), fmt.Sprintf("Case: [%d]", i))
		ensure.False(t, found, fmt.Sprintf("Case: [%d]", i))
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockFetchNonExisting(t *testing.T) {
	cases := []struct {
		db             *dataBlock
		startOffset    dataOffset
		key            string
		expectedErrStr string
	}{
		{ // Case-0: Key is not found.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(),
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (2), data size (2)
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x00)
					},
				),
			},
			startOffset:    0x10,
			key:            "NonExistingKey",
			expectedErrStr: `^dataBlock: key \[NonExistingKey\] was not found starting at \[10\]*`,
		},
	}

	for i, tc := range cases {
		data, found, err := tc.db.fetch(tc.startOffset, tc.key)
		ensure.True(t, (data == nil), fmt.Sprintf("Case: [%d]", i))
		ensure.False(t, found, fmt.Sprintf("Case: [%d]", i))
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockUpdates(t *testing.T) {
	cases := []struct {
		db                 *dataBlock
		startOffset        dataOffset
		key                string
		data               []byte
		expectedOffset     dataOffset
		expectedBlockState []byte
	}{
		{ // Case-0: Key is at the head of the list and the data is exact match.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x10, 0x00, 0x00, 0x00), // write offset (0x10)
					[]byte{
						0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // data size and key size
						0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
						0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
						0x00, 0x00, 0x00, 0x00, // next (0)
					},
				),
			},
			startOffset:    0x10,
			key:            "TestKey",
			data:           []byte("TESTTEST1"),
			expectedOffset: 0x10,
			expectedBlockState: concatBytes(
				headerBytes(0x10, 0x00, 0x00, 0x00), // write offset (0x24)
				[]byte{
					0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // data size and key size
					0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
					0x54, 0x45, 0x53, 0x54, 0x54, 0x45, 0x53, 0x54, 0x31, // data (TESTTEST1)
					0x00, 0x00, 0x00, 0x00, // next (0)
				},
			),
		},
		{ // Case-1: Key is at the head of the list and the new data size is different from previous one.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x20, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00), // write offset (0x20), total size (0x10)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x20, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x20)

						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
					},
				),
			},
			startOffset:    0x10,
			key:            "AA",
			data:           []byte("NewData"),
			expectedOffset: 0x20,
			expectedBlockState: concatBytes(
				headerBytes(0x35, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00), // write offset (0x35), total size (0x15)
				[]byte{
					// Abandoned record.
					0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (2), data size (2)
					0x41, 0x41, 0x31, 0x31, 0x20, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x20)

					// new record.
					0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, // key size (2), data size (7)
					0x41, 0x41, 0x4E, 0x65, 0x77, 0x44, 0x61, 0x74, 0x61, // key (AA), Data (NewData)
					0x20, 0x00, 0x00, 0x00, // next(0x20)
					0x00, 0x00, 0x00, // buffer
				},
			),
		},
		{ // Case-2: Key is at the middle of the list and the new data size is different from previous one.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x30, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00), // write offset (0x30), total size (0x20)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x20, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x20)

						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x42, 0x42, 0x32, 0x32, 0xF0, 0xE0, 0xD0, 0xC0, // key (BB), Data (22), next(0xC0D0E0F0)

						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
					},
				),
			},
			startOffset:    0x10,
			key:            "BB",
			data:           []byte("NewData"),
			expectedOffset: 0x10,
			expectedBlockState: concatBytes(
				headerBytes(0x45, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00), // write offset (0x45), total size (0x25)
				[]byte{
					0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (2), data size (2)
					0x41, 0x41, 0x31, 0x31, 0x30, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x30)

					// abandoned record.
					0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size (2), data size (2)
					0x42, 0x42, 0x32, 0x32, 0xF0, 0xE0, 0xD0, 0xC0, // key (BB), Data (22), next(0xC0D0E0F0)

					// New record.
					0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, // key size (2), data size (7)
					0x42, 0x42, 0x4E, 0x65, 0x77, 0x44, 0x61, 0x74, 0x61, // key (BB), Data (NewData)
					0xF0, 0xE0, 0xD0, 0xC0, //  next(0xC0D0E0F0)

					0x00, 0x00, 0x00, // buffer
				},
			),
		},
		{ // Case-3: Key was not found.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x20, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00), // write offset (0x20), total size (0x10)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x00)

						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
					},
				),
			},
			startOffset:    0x10,
			key:            "BB",
			data:           []byte("22"),
			expectedOffset: 0x10,
			expectedBlockState: concatBytes(
				headerBytes(0x30, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00), // write offset (0x30), total size (0x20)
				[]byte{
					0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
					0x41, 0x41, 0x31, 0x31, 0x20, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0x20)

					// new record.
					0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
					0x42, 0x42, 0x32, 0x32, 0x00, 0x00, 0x00, 0x00, // key (BB), Data (22), next(0x20)
				},
			),
		},
	}

	for i, tc := range cases {
		offset, err := tc.db.update(tc.startOffset, tc.key, tc.data)
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, offset, tc.expectedOffset, fmt.Sprintf("Case: [%d]", i))
		ensure.DeepEqual(t, tc.db.block, tc.expectedBlockState, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockUpdateErrors(t *testing.T) {
	cases := []struct {
		db             *dataBlock
		startOffset    dataOffset
		key            string
		data           []byte
		expectedErrStr string
	}{
		{ // case-0: out of bound start offset.
			db:             &dataBlock{},
			startOffset:    0xFF,
			key:            "key",
			data:           []byte("value"),
			expectedErrStr: `^dataBlock: Cannot read out of bound offset \[0xff\]. Block size: \[0x0\]*`,
		},
		{ // case-1: No space to update the list with a new key.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x20, 0x00, 0x00, 0x00), // write offset (0x20)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0)
					},
				),
			},
			startOffset:    0x10,
			key:            "key",
			data:           []byte("value"),
			expectedErrStr: `^dataBlock: Cannot write to offset \[0x20\]. Block size: \[0x20\]*`,
		},
		{ // case-2: No space to update the list. Key exists but the data doesn't fit.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x20, 0x00, 0x00, 0x00), // write offset (0x20)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0)

						0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // buffer
					},
				),
			},
			startOffset:    0x10,
			key:            "AA",
			data:           []byte("value"),
			expectedErrStr: `^dataBlock: Cannot write to offset \[0x20\]. Record \[0x13 bytes\] exceeds data block boundary \[0x28\]`,
		},
		{ // case-3: It's an existing key with bigger data. Cannot be added because of a bad write offset.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x05, 0x00, 0x00, 0x00), // write offset (0x20)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0)
					},
				),
			},
			startOffset:    0x10,
			key:            "AA",
			data:           []byte("value"),
			expectedErrStr: `^dataBlock: invalid write offset \[0x5\]. It falls within header area \[0x00 - 0x10\]`,
		},
		{ // case-3: It's a new key but cannot be added because of a bad write offset.
			db: &dataBlock{
				block: concatBytes(
					headerBytes(0x05, 0x00, 0x00, 0x00), // write offset (0x20)
					[]byte{
						0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, // key size, data size
						0x41, 0x41, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, // key (AA), Data (11), next(0)
					},
				),
			},
			startOffset:    0x10,
			key:            "key",
			data:           []byte("value"),
			expectedErrStr: `^dataBlock: invalid write offset \[0x5\]. It falls within header area \[0x00 - 0x10\]`,
		},
	}

	for i, tc := range cases {
		offset, err := tc.db.update(tc.startOffset, tc.key, tc.data)
		ensure.True(t, (offset == 0), fmt.Sprintf("Case: [%d]. offset [%#v] should be 0", i, offset))
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("Case: [%d]", i))
	}
}

// TestDataBlockSave tests successful saving of key value pairs in a given data block.
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
				headerBytes(0x24, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00), // write offset (0x24), total size (0x14)
				[]byte{
					0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, // key size (3), data size (5)
					0x6b, 0x65, 0x79, 0x76, 0x61, 0x6C, 0x75, 0x65, // Key (key), data (value)
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
				headerBytes(0x52, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00), // write offset (0x52), total size (0x42)
				[]byte{
					0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, // key size (4), data size (6)
					0x6b, 0x65, 0x79, 0x31, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x31, // key (key1), data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)

					0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, // key size (4), data size (6)
					0x6b, 0x65, 0x79, 0x33, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x33, // key (key1), data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)

					0x04, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, // key size (4), data size (6)
					0x6b, 0x65, 0x79, 0x32, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x32, // key (key1), data (value1)
					0x00, 0x00, 0x00, 0x00, // next (0)
				},
			),
		},
	}

	for i, tc := range cases {
		db := &dataBlock{block: make([]byte, len(tc.expectedBlock))}
		err := db.reset() // Reset the write offset.
		ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
		for _, key := range tc.order {
			val := tc.kvPairs[key]
			offset, err := db.save(key, val)
			ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
			ensure.True(t, (offset > 0), fmt.Sprintf("Case: [%d]. Offset must not be 0. Got [%x]", i, offset))
		}
		ensure.DeepEqual(t, db.block, tc.expectedBlock, fmt.Sprintf("Case: [%d]", i))
	}
}

func TestDataBlockSaveErrors(t *testing.T) {
	cases := []struct {
		keys           []string
		values         [][]byte
		blockSize      int
		expectedErrStr string
	}{
		{ // Case-0: empty keys.
			keys:           []string{""},
			values:         [][]byte{[]byte("123")},
			blockSize:      320,
			expectedErrStr: `^dataBlock: save failed. key \[\] and data \[31 32 33\] must be non-zero length*`,
		},
		{ // Case-1: empty values.
			keys:           []string{"key"},
			values:         [][]byte{[]byte{}},
			blockSize:      320,
			expectedErrStr: `^dataBlock: save failed. key \[key\] and data \[\] must be non-zero length*`,
		},
		{ // Case-2: Test saving when the block is completely full.
			keys:           []string{"key1", "key2", "key3", "key4"},
			values:         [][]byte{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4")},
			blockSize:      60,
			expectedErrStr: `^dataBlock: Cannot write to offset \[0x4c\]. Block size: \[0x4c\]*`,
		},
		{ // Case-3: Test saving when the free space is not sufficient to save a new record.
			keys:           []string{"key1", "key2", "key3", "key4"},
			values:         [][]byte{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4")},
			blockSize:      65,
			expectedErrStr: `^dataBlock: Cannot write to offset \[0x4c\]. Record \[0x14 bytes\] exceeds data block boundary \[0x51\]*`,
		},
	}

	for i, tc := range cases {
		db := &dataBlock{
			block: concatBytes(
				headerBytes(0x10, 0x00, 0x00, 0x00), // write offset (0x10)
				make([]byte, tc.blockSize),
			),
		}
		for j, key := range tc.keys {
			_, err := db.save(key, tc.values[j])
			// expect an error only while saving the last record.
			if j == len(tc.keys)-1 {
				ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("Case: [%d]", i))
			} else {
				// Saving all other keys should not result in error.
				ensure.Nil(t, err, fmt.Sprintf("Case: [%d]", i))
			}
		}
	}
}

// TestDataRecordWrite tests successfull writes into the given valid block.
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
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7), data size (9)
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

// TestDataRecordRead tests successfull reads into a dataRecord.
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
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7), data size (9)
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
				0x00, 0x00, 0x00, 0x00, // next (0)
			},
			expectedRec: &dataRecord{key: []byte("TestKey"), data: []byte("TestValue")},
		},
		{ // Case-3: Reading a non-empty record from a bigger block of data.
			block: []byte{
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7),  data size (9)
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
				0x54, 0x65, 0x73, 0x74, 0x56, 0x61, 0x6C, 0x75, 0x65, // data (TestValue)
				0x04, 0x03, 0x02, 0x01, // next (0)

				0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, // junk
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
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7), data size (9)
			},
			expectedErrStr: "^dataRecord: failed to read the key*",
		},
		{ // Case-3
			block: []byte{
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7), data size (9)
				0x54, 0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, // key (TestKey)
			},
			expectedErrStr: "^dataRecord: failed to read the data*",
		},
		{ // Case-4
			block: []byte{
				0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, // key size (7), data size (9)
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
