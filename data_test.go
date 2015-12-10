package dyconf

import (
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

// TestDataRecordSizeCalculation tests the data record size calculation.
func TestDataRecordSizeCalculation(t *testing.T) {
	cases := []struct {
		rec          *dataRecord
		expectedSize uint32
	}{
		{
			rec:          &dataRecord{},
			expectedSize: 14,
		},
		{
			rec:          &dataRecord{totalSize: 128},
			expectedSize: 128,
		},
		{
			rec: &dataRecord{
				totalSize: 20,
				key:       []byte(`abc`),
				data:      []byte(`123`),
				next:      0,
			},
			expectedSize: 20,
		},
	}

	for i, tc := range cases {
		ensure.DeepEqual(t, tc.rec.size(), tc.expectedSize, fmt.Sprintf("Case: [%d]", i))
	}
}
