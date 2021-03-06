package main

import (
	"fmt"
	"github.com/oxtoacart/framed"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	wg                   = sync.WaitGroup{}
	msgReceived          = make(chan int, 100)
	msgCount             = 0
	firstMessageReceived time.Time
	lastMessageReceived  time.Time

	NUM_CLIENTS      = intOrDefault("NUM_CLIENTS", 20)
	PEERS_PER_CLIENT = intOrDefault("PEERS_PER_CLIENT", 5)
	DIRECT_SPACING   = time.Duration(intOrDefault("DIRECT_SPACING", 50000)) * time.Microsecond
	STARTUP_SPACING  = time.Duration(intOrDefault("STARTUP_SPACING", 1000)) * time.Microsecond

	startReadingAt = time.Now().Add(time.Duration(NUM_CLIENTS) * STARTUP_SPACING * 2)
	stopReadingAt  = startReadingAt.Add(time.Duration(intOrDefault("RUNTIME", 10)) * time.Second)
)

func intOrDefault(name string, d int) int {
	val, err := strconv.Atoi(os.Getenv(name))
	if err != nil || val == 0 {
		val = d
	}
	return val
}

func TestClient(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg.Add(NUM_CLIENTS * 2) // *2 to accomodate read + write goroutines

	go func() {
		for {
			<-msgReceived
			if msgCount == 0 {
				firstMessageReceived = time.Now()
			}
			lastMessageReceived = time.Now()
			msgCount += 1
			if msgCount%10000 == 0 {
				fmt.Print(".")
			}
		}
	}()

	for i := 0; i < NUM_CLIENTS; i++ {
		go runTest(t, i)
		time.Sleep(STARTUP_SPACING)
	}

	wg.Wait()

	delta := lastMessageReceived.Sub(firstMessageReceived).Seconds()
	log.Printf("Received %d messages at %d mps", msgCount, float64(msgCount)/delta)
	os.Exit(0)
}

func runTest(t *testing.T, seq int) {
	addr := Addr(seq)

	conn := dialWaddell(t)

	go func() {
		for i := 0; i < PEERS_PER_CLIENT; i++ {
			to := peerFor(seq - i - 1)
			msg := mpool.Get()
			msg.Set(addr, to, OP_SUBSCRIBE, nil)
			if err := msg.WriteTo(conn); err != nil {
				t.Errorf("Unable to subscribe: %s", err)
			}
			msg.Release()
		}

		for i := 0; i < PEERS_PER_CLIENT; i++ {
			to := peerFor(seq + i + 1)
			msg := mpool.Get()
			msg.Set(addr, to, OP_APPROVE, nil)
			if err := msg.WriteTo(conn); err != nil {
				t.Errorf("Unable to approve subscription: %s", err)
			}
			msg.Release()
		}
	}()

	body := []byte("Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strX")

	time.Sleep(startReadingAt.Sub(time.Now()))

	go func() {
		defer wg.Done()

		for {
			continueFor := stopReadingAt.Sub(time.Now())
			if continueFor < time.Duration(0) {
				return
			}
			select {
			case <-time.After(continueFor):
				return
			default:
				msg := mpool.Get()
				err := msg.ReadFrom(conn)
				msg.Release()
				if err == nil || err == io.EOF {
					msgReceived <- 1
				} else {
					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer conn.Close()

		i := 0
		for {
			continueFor := stopReadingAt.Sub(time.Now())
			if continueFor < time.Duration(0) {
				return
			}
			select {
			case <-time.After(continueFor):
				return
			default:
				msg := mpool.Get()
				to := peerFor(seq - (i % PEERS_PER_CLIENT) - 1)
				msg.Set(addr, to, OP_SEND, body)
				if err := msg.WriteTo(conn); err != nil {
					t.Errorf("Unable to write message body: %s", err)
				}
				msg.Release()
				time.Sleep(DIRECT_SPACING)
				i++
			}
		}
	}()
}

func dialWaddell(t *testing.T) *framed.Framed {
	conn, err := net.Dial("tcp", getAddr())
	if err != nil {
		t.Fatalf("Unable to dial waddell")
	}
	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetWriteBuffer(WRITE_BUFFER_BYTES); err != nil {
		log.Printf("Unable to set write buffer, sticking with default")
	}
	if err := tcpConn.SetReadBuffer(READ_BUFFER_BYTES); err != nil {
		log.Printf("Unable to set read buffer, sticking with default")
	}
	return &framed.Framed{conn}
}

func peerFor(i int) Addr {
	if i < 0 {
		i += NUM_CLIENTS
	} else if i >= NUM_CLIENTS {
		i -= NUM_CLIENTS
	}
	return Addr(i)
}
