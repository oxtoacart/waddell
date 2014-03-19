package main

import (
	"fmt"
	"github.com/oxtoacart/framed"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	NUM_CLIENTS         = 20
	PEERS_PER_CLIENT    = 5
	NUM_BROADCASTS      = 250
	BROADCAST_SPACING   = 8 * time.Millisecond
	NUM_DIRECT_MESSAGES = 2500
	DIRECT_SPACING      = 20 * time.Microsecond
	STARTUP_SPACING     = 20 * time.Millisecond
)

var (
	wg                   = sync.WaitGroup{}
	subscribeWg          = sync.WaitGroup{}
	approveWg            = sync.WaitGroup{}
	msgReceived          = make(chan int, 100)
	msgCount             = 0
	firstMessageReceived time.Time
	lastMessageReceived  time.Time
)

func TestClient(t *testing.T) {
	runtime.GOMAXPROCS(1)
	wg.Add(NUM_CLIENTS)
	subscribeWg.Add(NUM_CLIENTS)
	approveWg.Add(NUM_CLIENTS)

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
}

func runTest(t *testing.T, seq int) {
	addr := Addr(seq)
	peers := make([]Addr, PEERS_PER_CLIENT)
	for i := 0; i < PEERS_PER_CLIENT; i++ {
		peer := seq - i
		if peer < 0 {
			peer += NUM_CLIENTS
		}
		peers[i] = Addr(peer)
	}

	conn := dialWaddell(t)
	defer conn.Close()

	for _, peer := range peers {
		msg := &Message{
			from: addr,
			to:   peer,
			op:   OP_SUBSCRIBE,
		}
		if err := msg.writeHeaderTo(conn, uint16(0)); err != nil {
			t.Errorf("Unable to subscribe: %s", err)
		}
	}
	subscribeWg.Done()

	for _, peer := range peers {
		msg := &Message{
			from: addr,
			to:   peer,
			op:   OP_APPROVE,
		}
		if err := msg.writeHeaderTo(conn, uint16(0)); err != nil {
			t.Errorf("Unable to approve subscription: %s", err)
		}
	}
	approveWg.Done()

	subscribeWg.Wait()
	approveWg.Wait()

	time.Sleep(2 * time.Second)

	body := []byte("Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello str")
	bodyLength := uint16(len(body))
	msg := &Message{
		from: addr,
		to:   Addr(0),
		op:   OP_PUBLISH,
	}

	//finishedBroadcasting := make(chan bool)
	finishedSending := make(chan bool)

	go func() {
		frame, err := conn.ReadInitial()
		if err != nil {
			t.Errorf("Unable to read initial frame: %s", err)
			return
		}
		for {
			// Discard the frame
			frame.Discard()
			msgReceived <- 1
			frame, err = frame.Next()
			if err != nil {
				if err != io.EOF {
					t.Errorf("Unable to read next frame: %s", err)
				}
				return
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
		for i := 0; i < NUM_DIRECT_MESSAGES; i++ {
			msgTo := msg.withTo(peers[i%PEERS_PER_CLIENT])
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
		finishedSending <- true
	}()

	//<-finishedBroadcasting
	<-finishedSending

	time.Sleep(10 * time.Second)

	wg.Done()
}

func dialWaddell(t *testing.T) *framed.Framed {
	conn, err := net.Dial("tcp", WADDELL_ADDR)
	if err != nil {
		t.Fatalf("Unable to dial waddell")
	}
	return framed.NewFramed(conn)
}

func reconnect(t *testing.T, conn *framed.Framed) *framed.Framed {
	conn.Close()
	return dialWaddell(t)
}
