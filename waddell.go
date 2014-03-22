package main

import (
	"encoding/binary"
	"github.com/oxtoacart/bpool"
	"github.com/oxtoacart/framed"
	"io"
	"log"
	"net"
	"os"
	"runtime"
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
	bufferPool = bpool.NewBytePool(1000000, 200)
)

// Client represents a client of waddell's
type Client struct {
	addr                  Addr
	conn                  *framed.Framed
	subscriptionRequests  map[Addr]bool // requests to subscribe to Client's messages
	subscriptionApprovals map[Addr]bool // permission to subscribe to Client's messages
	subscriptions         map[Addr]bool
	msgFrom               chan *Message
	msgTo                 chan *Message
}

type Message struct {
	from  Addr
	to    Addr
	op    Op
	frame *framed.Frame
}

type MessageWithConn struct {
	msg  *Message
	conn *framed.Framed
}

var (
	clients     = make(map[Addr]*Client)
	messagesIn  = make(chan *MessageWithConn, 10000)
	messagesOut = make(chan *Message, 10000)
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
				framed := framed.NewFramed(conn, bufferPool)
				defer conn.Close()
				for {
					frame, err := framed.ReadFrame()
					if err == io.EOF {
						return
					} else if err != nil {
						log.Printf("Unable to read next message: %s", err)
					} else {
						messagesIn <- &MessageWithConn{msg: newMessage(frame), conn: framed}
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
			from := clients[in.msg.from]
			if from == nil {
				from = &Client{
					addr:                  in.msg.from,
					subscriptionRequests:  make(map[Addr]bool),
					subscriptionApprovals: make(map[Addr]bool),
					subscriptions:         make(map[Addr]bool),
					msgFrom:               make(chan *Message, 1),
					msgTo:                 make(chan *Message, 100),
				}
				go from.dispatch()
				clients[in.msg.from] = from
			}
			from.conn = in.conn
			from.msgFrom <- in.msg
		case out := <-messagesOut:
			to := clients[out.to]
			if to == nil {
				to = &Client{
					addr:                  out.to,
					subscriptionRequests:  make(map[Addr]bool),
					subscriptionApprovals: make(map[Addr]bool),
					subscriptions:         make(map[Addr]bool),
					msgFrom:               make(chan *Message, 1),
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
			switch out.op {
			case OP_APPROVE:
				defer out.frame.Release()
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
		case in := <-client.msgTo:
			defer in.frame.Release()
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
				if client.conn != nil { // TODO: make sure from is actually approved
					if err := client.conn.WriteFrame(in.frame.Buffers...); err != nil {
						if err == io.EOF {
							client.conn = nil
						} else {
							log.Printf("Unable to send message from %s to %s: %s", in.from, in.to, err)
						}
					}
				} else if client.conn == nil {
					log.Printf("Client %s has no connection", client.addr)
				} else {
					log.Printf("%s is not approved to send to %s", in.from, in.to)
				}
			}
		}
	}
}

func newMessage(frame *framed.Frame) *Message {
	buffer := frame.Buffers[0]
	return &Message{
		frame: frame,
		from:  Addr(endianness.Uint64(buffer[0:8])),
		to:    Addr(endianness.Uint64(buffer[8:16])),
		op:    Op(endianness.Uint16(buffer[16:18])),
	}
}

func (msg *Message) withTo(to Addr) *Message {
	return &Message{
		from:  msg.from,
		to:    to,
		op:    OP_SEND,
		frame: msg.frame,
	}
}

func getAddr() string {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = "127.0.0.1:10080"
	}
	return addr
}
