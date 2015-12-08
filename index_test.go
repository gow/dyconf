package dyconf

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"regexp"
	"strconv"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestSuccessfulGet(t *testing.T) {
	cases := []struct {
		indexBlk        indexBlock
		mockedHashIndex uint32
		expectedDataPtr dataPtr
	}{
		{ // Case-0: index block contains 0x11223344 at 0th index
			indexBlk:        indexBlock{size: 4, data: []byte{0x44, 0x33, 0x22, 0x11}},
			mockedHashIndex: 0,
			expectedDataPtr: 0x11223344,
		},

		{ // Case-1: index block contains 0x11223344 at 1st index
			indexBlk:        indexBlock{size: 4, data: []byte{0x0, 0x0, 0x0, 0x0, 0x44, 0x33, 0x22, 0x11}},
			mockedHashIndex: 1,
			expectedDataPtr: 0x11223344,
		},
		{ // Case-2: index block contains 0x44332211 at 100th index
			indexBlk: indexBlock{
				size: 110,
				data: append(make([]byte, sizeOfUint32*100), []byte{0x11, 0x22, 0x33, 0x44, 0x0, 0x0, 0x0, 0x0}...),
			},
			mockedHashIndex: 100,
			expectedDataPtr: 0x44332211,
		},
	}

	for i, tc := range cases {
		savedHashFunc := defaultHashFunc
		defaultHashFunc = func(s string) (uint32, error) {
			return tc.mockedHashIndex, nil
		}
		ptr, err := tc.indexBlk.get("qwerty")
		ensure.Nil(t, err)
		ensure.DeepEqual(t, ptr, tc.expectedDataPtr, fmt.Sprintf("case: %d", i), "Input: ", tc.indexBlk)
		defaultHashFunc = savedHashFunc
	}
}

func TestGetErrors(t *testing.T) {
	savedHashFunc := defaultHashFunc
	indexBlk := indexBlock{}

	// Fake an error in hashing
	defaultHashFunc = func(s string) (uint32, error) {
		return 0, fmt.Errorf("Fake hashing error")
	}
	_, err := indexBlk.get("hello")
	ensure.Err(t, err, regexp.MustCompile("Fake hashing error"))
	defaultHashFunc = savedHashFunc
}

/******************* Benchmarks ****************************/
func BenchmarkHashFNV1a(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h := fnv.New32a()
		h.Write([]byte(strconv.Itoa(i)))
		h.Sum32()
	}
}

func BenchmarkHashCRC32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h := crc32.NewIEEE()
		h.Write([]byte(strconv.Itoa(i)))
		h.Sum32()
	}
}
