package dyconf

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/facebookgo/ensure"
)

func BenchmarkDyconfSet(b *testing.B) {
	// Setup
	tmpFile, err := ioutil.TempFile("", "dyconf-BenchMarkDyconfSet")
	ensure.Nil(b, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpFileName)

	// Set the keys in the given sequence.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		err = wc.Set(key, []byte(val))
		if err != nil {
			break
		}
	}
	ensure.Nil(b, err)
}

func BenchmarkDyconfGet(b *testing.B) {
	// Setup
	tmpFile, err := ioutil.TempFile("", "dyconf-BenchMarkDyconfGet")
	ensure.Nil(b, err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	os.Remove(tmpFileName)

	// Set the keys first.
	wc, err := NewManager(tmpFileName)
	ensure.Nil(b, err)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		err = wc.Set(key, []byte(val))
		if err != nil {
			break
		}
	}
	ensure.Nil(b, err)

	// Now reset the timer and start reading the keys.
	conf, err := New(tmpFileName)
	ensure.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, err = conf.Get(key)
		if err != nil {
			break
		}
	}
	ensure.Nil(b, err)
}

func BenchmarkSimpleMapGet(b *testing.B) {
	kvMap := make(map[string]string)
	// Set the keys first.
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		kvMap[key] = val
	}

	// Now reset the timer and start reading the keys.
	b.ResetTimer()
	m := sync.Mutex{}
	ok := true
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		m.Lock()
		_, ok = kvMap[key]
		m.Unlock()
		if !ok {
			break
		}
	}
	ensure.True(b, ok)
}

/***************** Flock ********************/
func BenchmarkFlockRead(b *testing.B) {
	lockFile := "/tmp/unit_test_lock_file"
	file, err := os.Create(lockFile)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
		if err != nil {
			b.Fatal(err)
		}
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	}
}

/***************** sync.RWMutext ********************/
func BenchmarkSyncLockRead(b *testing.B) {
	m := sync.RWMutex{}

	for i := 0; i < b.N; i++ {
		m.RLock()
		m.RUnlock()
	}
}

/***************** RPC ********************/
const hostStr = "localhost:0"

func BenchmarkRPCReadsWithNewConn(b *testing.B) {
	l, err := setupRPCServer()
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := rpc.Dial("tcp", l.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		args := &Args{9, 2}
		var mul int64
		err = c.Call("Arith.Mul", args, &mul)
		c.Close()
		if err != nil {
			b.Fatal(err)
		}
		if mul != int64(args.A*args.B) {
			b.Fatalf("Expected: %d, Received: %d", args.A*args.B, mul)
		}
	}
}
func BenchmarkRPCReadsWithExistConn(b *testing.B) {
	l, err := setupRPCServer()
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	c, err := rpc.Dial("tcp", l.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := &Args{9, 2}
		var mul int64
		err = c.Call("Arith.Mul", args, &mul)
		if err != nil {
			b.Fatal(err)
		}
		if mul != int64(args.A*args.B) {
			b.Fatalf("Expected: %d, Received: %d", args.A*args.B, mul)
		}
	}
}

type Args struct {
	A, B int
}

type Arith struct{}

func (
	t *Arith) Mul(args *Args, mul *int64) error {
	*mul = int64(args.A * args.B)
	return nil
}
func (t *Arith) Div(args *Args, div *float64) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	*div = float64(args.A / args.B)
	return nil
}

func setupRPCServer() (net.Listener, error) {
	arith := new(Arith)
	rs := rpc.NewServer()
	rs.Register(arith)
	l, err := net.Listen("tcp", hostStr)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				//panic(err)
				break
			} else {
				go rs.ServeConn(conn)
			}
		}
	}()
	return l, nil
}
