package otfc

import (
	//"crypto/md5"
	"log"
	//"fmt"
	"errors"
	"os"
	"syscall"
	"unsafe"
)

const (
	INDEX_SIZE      = 1023
	DATA_BLOCK_SIZE = 256 * 1024 // 256k bytes
	CONFIG_FILE     = "/tmp/71ebdf319f2a7fa1d4eb45f9c4b7cf64"
)

type Config struct {
	header configHeader
	index  indexBlock
	data   [DATA_BLOCK_SIZE]byte
}

type OTFC struct {
	configPtr *Config
}

var configPtr *Config

// Returns the count of the config records. If the given OTFC is not initialized, an error is returned.
func (otfc *OTFC) NumRecords() (count uint32, err error) {
	//log.Println(otfc)
	if otfc.configPtr == nil {
		log.Println("Uninitialized config file")
		err = errors.New("Uninitialized config file")
		return
	} else {
		count = otfc.configPtr.header.NumRecords()
	}
	return
}

// Initializes the config.
func Init(fileName string) (err error) {
	//log.Printf("%#p\n", otfc.configPtr)
	size := int32(24 /* header size */ + (24 * INDEX_SIZE) /* 24 bytes X 1023 index records */ + DATA_BLOCK_SIZE /* Size of the config data block */)
	log.Printf("Size: %#d\n", size)
	//mapFileFD, err := createFile(fileName, size)
	mapFile, err := os.Open(fileName)
	if err != nil {
		return
	}
	// mmap the config file.
	mmap, err := syscall.Mmap(int(mapFile.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		mapFile.Close()
		log.Printf("%s\n", err)
		return
	}
	// Make sure mmap gave us enough memory.
	if len(mmap) < int(size) {
		err = errors.New("Insufficient memmory")
		mapFile.Close()
		return
	}
	// Convert the byte array to Config struct type.
	configPtr = (*Config)(unsafe.Pointer(&mmap[0]))
	return
}

/*
func createFile(fileName string, size int32) (file *os.File, err error) {
	file, err = os.Create(fileName)
	if err != nil {
		return
	}
	_, err = file.Seek(int64(size), 0)
	if err != nil {
		log.Println(err)
		return
	}

	_, err = file.Write([]byte("x"))
	if err != nil {
		log.Println(err)
		return
	}
	return
}

*/
func (otfc *OTFC) Get(key string) (value []byte, err error) {
	// TODO implement get.
	/*
	  h := md5.New()
	  h.Write([]byte(key))
	  hash := h.Sum(nil)

	  offset, err := otfc.index.Find(hash)
	  if err != nil {
	    return
	  }
	*/
	return
}

/*
// Sets the given config key and value pair.
func (otfc *OTFC) Set(key string, value []byte) (err error) {
	count, _ := otfc.NumRecords()
	var rec indexRecord

	// md5(key)
	h := md5.New()
	h.Write([]byte(key))
	keyHash := h.Sum(nil)

	// Create new index record.
	copyLen := copy(rec.key[:], keyHash)
	log.Println("Copy len: ", copyLen)
	rec.offset = otfc.configPtr.header.writeOffset
	rec.length = uint32(len(value))
	otfc.configPtr.index[count] = rec

	otfc.configPtr.header.writeOffset = rec.offset + rec.length
	otfc.configPtr.header.SetRecordCount(count + 1)
	return
}

func (otfc *OTFC) PrintHeader() {
	otfc.configPtr.header.print()
}

*/
func PrintIndexBlock() {
	configPtr.index.print()
	//for _, x := range otfc.configPtr.index {
	//x.print()
	//}
}
