package dyconf

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"testing"
)

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

func BenchmarkRPCReads(b *testing.B) {
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
