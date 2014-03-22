package main

import (
	"fmt"
	"github.com/oxtoacart/framed"
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
	stopReadingAt  = startReadingAt.Add(10 * time.Second)
)

func intOrDefault(name string, d int) int {
	val, err := strconv.Atoi(os.Getenv(name))
	if err != nil || val == 0 {
		val = d
	}
	return val
}

func TestClient(t *testing.T) {
	runtime.GOMAXPROCS(2)
	wg.Add(NUM_CLIENTS)

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
	peers := make([]Addr, PEERS_PER_CLIENT)
	for i := 0; i < PEERS_PER_CLIENT; i++ {
		peer := seq - i - 1
		if peer < 0 {
			peer += NUM_CLIENTS
		}
		peers[i] = Addr(peer)
	}

	conn := dialWaddell(t)

	go func() {
		for _, peer := range peers {
			msg := &Message{
				from: addr,
				to:   peer,
				op:   OP_SUBSCRIBE,
			}
			if err := msg.writeHeaderTo(conn, uint16(0)); err != nil {
				t.Errorf("Unable to subscribe: %s", err)
			}

			msg.op = OP_APPROVE
			if err := msg.writeHeaderTo(conn, uint16(0)); err != nil {
				t.Errorf("Unable to approve subscription: %s", err)
			}
		}
	}()

	body := []byte("Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello str")
	bodyLength := uint16(len(body))
	msg := &Message{
		from: addr,
		to:   Addr(0),
		op:   OP_PUBLISH,
	}

	time.Sleep(startReadingAt.Sub(time.Now()))

	go func() {
		defer wg.Done()

		frame, err := conn.ReadInitial()
		if err != nil {
			t.Errorf("Unable to read initial frame: %s", err)
		}
		for {
			continueFor := stopReadingAt.Sub(time.Now())
			select {
			case <-time.After(continueFor):
				return
			default:
				// Discard the frame
				frame.Discard()
				msgReceived <- 1
				frame, err = frame.Next()
				if err != nil {
					return
				}
			}
		}
	}()

	// go func() {
	// 	for i := 0; i < NUM_BROADCASTS; i++ {
	// 		if err := conn.Write(encodeMessage(msg)); err != nil {
	// 			t.Errorf("Unable to publish message: %s", err)
	// 		}
	// 		time.Sleep(BROADCAST_SPACING)
	// 	}
	// 	finishedBroadcasting <- true
	// }()

	go func() {
		defer conn.Close()
		defer wg.Done()

		i := 0
		for {
			continueFor := stopReadingAt.Sub(time.Now())
			select {
			case <-time.After(continueFor):
				return
			default:
				msgTo := msg.withTo(peers[i%PEERS_PER_CLIENT])
				i++
				if err := msgTo.writeHeaderTo(conn, bodyLength); err != nil {
					t.Errorf("Unable to send message: %s", err)
					break
				}
				if _, err := conn.Write(body); err != nil {
					t.Errorf("Unable to write message body: %s", err)
					break
				}
				time.Sleep(DIRECT_SPACING)
			}
		}
	}()
}

func dialWaddell(t *testing.T) *framed.Framed {
	conn, err := net.Dial("tcp", WADDELL_ADDR)
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
	if err := tcpConn.SetNoDelay(false); err != nil {
		log.Printf("Unable to enable Nagle's algorithm, leaving off")
	}
	return framed.NewFramed(conn)
}

func reconnect(t *testing.T, conn *framed.Framed) *framed.Framed {
	conn.Close()
	return dialWaddell(t)
}
