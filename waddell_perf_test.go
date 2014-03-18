package main

import (
	"fmt"
	"github.com/oxtoacart/ftcp"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	NUM_CLIENTS         = 10000
	PEERS_PER_CLIENT    = 5
	NUM_BROADCASTS      = 25
	BROADCAST_SPACING   = 800 * time.Millisecond
	NUM_DIRECT_MESSAGES = 250
	DIRECT_SPACING      = 800 * time.Millisecond
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
	runtime.GOMAXPROCS(2)
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
	addr := fmt.Sprintf("%d@waddell.waddell", seq)
	peers := make([]string, PEERS_PER_CLIENT)
	for i := 0; i < PEERS_PER_CLIENT; i++ {
		peer := seq - i
		if peer < 0 {
			peer += NUM_CLIENTS
		}
		peers[i] = fmt.Sprintf("%d@waddell.waddell", peer)
	}

	conn := dialWaddell(t)
	defer conn.Close()

	for _, peer := range peers {
		msg := &Message{
			sender:    addr,
			recipient: peer,
			op:        OP_SUBSCRIBE,
		}
		if err := conn.Write(encodeMessage(msg)); err != nil {
			t.Errorf("Unable to subscribe: %s", err)
		}
	}
	subscribeWg.Done()

	for _, peer := range peers {
		msg := &Message{
			sender:    addr,
			recipient: peer,
			op:        OP_APPROVE,
		}
		if err := conn.Write(encodeMessage(msg)); err != nil {
			t.Errorf("Unable to approve subscription: %s", err)
		}
	}
	approveWg.Done()

	subscribeWg.Wait()
	approveWg.Wait()

	time.Sleep(2 * time.Second)

	data := []byte("Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world Hello strange signaling world ")
	msg := &Message{
		sender:    addr,
		recipient: "",
		op:        OP_PUBLISH,
		data:      data,
	}

	finishedBroadcasting := make(chan bool)
	finishedSending := make(chan bool)

	go func() {
		reader := conn.Reader()
		for {
			if _, err := reader.Read(); err != nil {
				return
			} else {
				msgReceived <- 1
			}
		}
	}()

	go func() {
		for i := 0; i < NUM_BROADCASTS; i++ {
			if err := conn.Write(encodeMessage(msg)); err != nil {
				t.Errorf("Unable to publish message: %s", err)
			}
			time.Sleep(BROADCAST_SPACING)
		}
		finishedBroadcasting <- true
	}()

	go func() {
		for i := 0; i < NUM_DIRECT_MESSAGES; i++ {
			if err := conn.Write(encodeMessage(msg.to(peers[i%PEERS_PER_CLIENT]))); err != nil {
				t.Errorf("Unable to send message: %s", err)
			}
			time.Sleep(DIRECT_SPACING)
		}
		finishedSending <- true
	}()

	<-finishedBroadcasting
	<-finishedSending

	time.Sleep(10 * time.Second)

	wg.Done()
}

func dialWaddell(t *testing.T) *ftcp.Conn {
	conn, err := ftcp.Dial(WADDELL_ADDR)
	if err != nil {
		t.Fatalf("Unable to dial waddell")
	}
	return conn
}

func reconnect(t *testing.T, conn *ftcp.Conn) *ftcp.Conn {
	conn.Close()
	return dialWaddell(t)
}
