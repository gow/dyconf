package dyconf

import (
	"hash/crc32"
	"hash/fnv"
	"strconv"
	"testing"
)

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
