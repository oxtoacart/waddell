package main

import (
	"encoding/binary"
	"fmt"
	"github.com/oxtoacart/framed"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

type Addr uint64
type Op uint16

const (
	OP_SUBSCRIBE         = Op(1)
	OP_APPROVE           = Op(2)
	OP_SEND              = Op(3)
	OP_PUBLISH           = Op(4)
	FRAMED_HEADER_LENGTH = 18

	WRITE_BUFFER_BYTES = 8096
	READ_BUFFER_BYTES  = 8096
)

var (
	endianness = binary.LittleEndian
	mpool      = NewMessagePool(100000, 1024) // Size this based on the transaction rate
)

// Client represents a client of waddell's
type Client struct {
	addr                  Addr
	conn                  io.ReadWriteCloser
	subscriptionRequests  map[Addr]bool // requests to subscribe to Client's messages
	subscriptionApprovals map[Addr]bool // permission to subscribe to Client's messages
	subscriptions         map[Addr]bool
	msgFrom               chan *Message
	msgTo                 chan *Message
}

var (
	clients       = make(map[Addr]*Client)
	messagesIn    = make(chan *Message, 100000) // size this based on the max expected message rate
	messagesOut   = make(chan *Message, 100000) // size this based on the max expected message rate
	profileTicker = time.NewTicker(30 * time.Second)
)

func HeaderFor(from Addr, to Addr, op Op) []byte {
	header := make([]byte, 18)
	endianness.PutUint64(header, uint64(from))
	endianness.PutUint64(header[8:16], uint64(to))
	endianness.PutUint16(header[16:18], uint16(op))
	return header
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	listener, err := net.Listen("tcp", getAddr())
	if err != nil {
		log.Fatalf("Unable to listen: %s", err)
	}
	defer listener.Close()

	go dispatch()

	go profile()

	log.Printf("Listening at %s", getAddr())
	// Accept connections, read message and respond
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Unable to accept: %s", err)
		} else {
			tcpConn := conn.(*net.TCPConn)
			if err := tcpConn.SetWriteBuffer(WRITE_BUFFER_BYTES); err != nil {
				log.Printf("Unable to set write buffer, sticking with default")
			}
			if err := tcpConn.SetReadBuffer(READ_BUFFER_BYTES); err != nil {
				log.Printf("Unable to set read buffer, sticking with default")
			}
			go func() {
				framed := &framed.Framed{conn}
				defer framed.Close()
				for {
					msg := mpool.Get()
					if err := msg.ReadFrom(framed); err != nil {
						// TODO: log error to debug log
						if err != io.EOF {
							log.Printf("Unable to read next message: %s", err)
						}
						msg.Release()
						return
					} else {
						messagesIn <- msg
					}
				}
			}()
		}
	}
}

func dispatch() {
	for {
		select {
		case in := <-messagesIn:
			from := clients[in.from]
			if from == nil {
				from = &Client{
					addr:                  in.from,
					subscriptionRequests:  make(map[Addr]bool),
					subscriptionApprovals: make(map[Addr]bool),
					subscriptions:         make(map[Addr]bool),
					msgFrom:               make(chan *Message, 10),
					msgTo:                 make(chan *Message, 100),
				}
				go from.dispatch()
				clients[in.from] = from
			}
			from.conn = in.conn
			from.msgFrom <- in
		case out := <-messagesOut:
			to := clients[out.to]
			if to == nil {
				to = &Client{
					addr:                  out.to,
					subscriptionRequests:  make(map[Addr]bool),
					subscriptionApprovals: make(map[Addr]bool),
					subscriptions:         make(map[Addr]bool),
					msgFrom:               make(chan *Message, 10),
					msgTo:                 make(chan *Message, 100),
				}
				go to.dispatch()
				clients[out.to] = to
			}
			to.msgTo <- out
		}
	}
}

func (client *Client) dispatch() {
	for {
		select {
		case out := <-client.msgFrom:
			client.handleOutbound(out)
		case in := <-client.msgTo:
			client.handleInbound(in)
		}
	}
}

func (client *Client) handleOutbound(out *Message) {
	switch out.op {
	case OP_APPROVE:
		defer out.Release()
		client.subscriptionApprovals[out.to] = true
		if client.subscriptionRequests[out.to] {
			client.subscriptions[out.to] = true
		}
	case OP_SUBSCRIBE, OP_SEND:
		messagesOut <- out
	case OP_PUBLISH:
		// TODO: handle publishing
		// for to, _ := range client.subscriptions {
		// 	messagesOut <- out.withTo(to)
		// }
	}
}

func (client *Client) handleInbound(in *Message) {
	defer in.Release()
	switch in.op {
	case OP_APPROVE:
		log.Printf("Got request to approve subscription from a different user - this shouldn't happen")
	case OP_SUBSCRIBE:
		client.subscriptionRequests[in.from] = true
		if client.subscriptionApprovals[in.from] {
			// Complete subscription
			client.subscriptions[in.from] = true
		}
	case OP_SEND:
		if client.subscriptions[in.from] {
			if client.conn != nil { // TODO: make sure from is actually approved
				if err := in.WriteTo(client.conn); err != nil {
					if err == io.EOF {
						client.conn = nil
					} else {
						log.Printf("Unable to send message from %s to %s: %s", in.from, in.to, err)
					}
				}
			} else if client.conn == nil {
				log.Printf("Client %s has no connection", client.addr)
			}
		} else {
			log.Printf("%s is not approved to send to %s", in.from, in.to)
		}
	}
}

// profile periodically dumps a heap profile in /tmp/waddell_heap_<timestamp>.mprof
func profile() {
	for {
		select {
		case <-profileTicker.C:
			runtime.GC()
			f, err := os.Create(fmt.Sprintf("/tmp/waddell_heap_%d.mprof", time.Now().Unix()))
			if err != nil {
				log.Printf("Unable to write heap profile: %s", err)
			}
			pprof.WriteHeapProfile(f)
		}
	}
}

func getAddr() string {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = "0.0.0.0:10080"
	}
	return addr
}
