package dyconf

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/facebookgo/ensure"
)

// TestHeaderRead tests the reading of header.
func TestHeaderRead(t *testing.T) {
	cases := []struct {
		inputBlock  []byte
		expectedHdr *headerBlock
	}{
		{
			inputBlock: []byte{
				0xAA, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, // Version(0xAA), totalSze(0xFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0x00, 0x00, 0x00, 0x00, // modifiedTime(0xAABBCCDD)
				0x00, 0xBB, 0x00, 0xAA, 0xFF, 0xFF, 0x00, 0x00, // indexBlockOffset(0xAA00BB00), indexBlockSize(0xFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0x00, 0xFF, 0x00, 0x00, // dataBlockOffset(0xAABBCCDD), dataBlockSize(0xFF00)
			},
			expectedHdr: &headerBlock{
				version:          0xAA,
				totalSize:        0xFFFF,
				modifiedTime:     time.Unix(0xAABBCCDD, 0),
				indexBlockOffset: 0xAA00BB00,
				indexBlockSize:   0xFFFF,
				dataBlockOffset:  0xAABBCCDD,
				dataBlockSize:    0xFF00,
			},
		},
	}

	for i, tc := range cases {
		hdr, err := (&headerBlock{}).read(tc.inputBlock)
		ensure.Nil(t, err, fmt.Sprintf("TestHeaderRead-Case%d-", i))

		hdr.block = nil //make this nil so that we can deep-compare the structures.
		ensure.DeepEqual(t, hdr, tc.expectedHdr, fmt.Sprintf("TestHeaderRead-Case%d-", i))
	}
}

// TestHeaderReadErrors tests for errors while reading the header.
func TestHeaderReadErrors(t *testing.T) {
	cases := []struct {
		inputBlock     []byte
		expectedErrStr string
	}{
		{ //Case-0: Incomplete header.
			inputBlock: []byte{
				0xAA, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, // Version(0xAA), totalSze(0xFFFF)
			},
			expectedErrStr: `^headerBlock: failed to read the header. It should be \[32\] bytes.`,
		},
		{ //Case-1: Index block size exceeds max allowed size.
			inputBlock: []byte{
				0xAA, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, // Version(0xAA), totalSze(0xFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0x00, 0x00, 0x00, 0x00, // modifiedTime(0xAABBCCDD)
				0x00, 0xBB, 0x00, 0xAA, 0xFF, 0xFF, 0xFF, 0xFF, // indexBlockOffset(0xAA00BB00), indexBlockSize(0xFFFFFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0x00, 0xFF, 0x00, 0x00, // dataBlockOffset(0xAABBCCDD), dataBlockSize(0xFF00)
			},
			expectedErrStr: `^headerBlock: invalid index block size \[0XFFFFFFFF\]. It should not exceed \[0X8000000\]`,
		},
		{ //Case-2: Data block size exceeds max allowed size.
			inputBlock: []byte{
				0xAA, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, // Version(0xAA), totalSze(0xFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0x00, 0x00, 0x00, 0x00, // modifiedTime(0xAABBCCDD)
				0x00, 0xBB, 0x00, 0xAA, 0xFF, 0xFF, 0x00, 0x00, // indexBlockOffset(0xAA00BB00), indexBlockSize(0xFFFF)
				0xDD, 0xCC, 0xBB, 0xAA, 0xFF, 0xFF, 0xFF, 0xFF, // dataBlockOffset(0xAABBCCDD), dataBlockSize(0xFFFFFFFF)
			},
			expectedErrStr: `^headerBlock: invalid data block size \[0XFFFFFFFF\]. It should not exceed \[0X40000000\]`,
		},
	}

	for i, tc := range cases {
		_, err := (&headerBlock{}).read(tc.inputBlock)
		ensure.Err(t, err, regexp.MustCompile(tc.expectedErrStr), fmt.Sprintf("TestHeaderRead-Case%d-", i))
	}
}

// TestHeaderSaveErrors tests errors while saving the header.
func TestHeaderSaveErrors(t *testing.T) {
	buf := make([]byte, headerBlockSize-1)
	hdr := &headerBlock{block: buf}
	err := hdr.save()
	ensure.Err(t, err, regexp.MustCompile(`headerBlock: failed to save the header. It should be \[\d+\] bytes.`))
}
