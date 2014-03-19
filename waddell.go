package main

import (
	"encoding/binary"
	"github.com/oxtoacart/framed"
	"io"
	"log"
	"net"
	"runtime"
)

type Addr uint64
type Op uint16

const (
	OP_SUBSCRIBE         = Op(1)
	OP_APPROVE           = Op(2)
	OP_SEND              = Op(3)
	OP_PUBLISH           = Op(4)
	WADDELL_ADDR         = "127.0.0.1:10080"
	FRAMED_HEADER_LENGTH = 18
)

var endianness = binary.LittleEndian

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
	messagesIn  = make(chan *MessageWithConn, 100000)
	messagesOut = make(chan *Message, 100000)
)

func main() {
	runtime.GOMAXPROCS(2)
	listener, err := net.Listen("tcp", WADDELL_ADDR)
	if err != nil {
		log.Fatalf("Unable to listen: %s", err)
	}
	defer listener.Close()

	go dispatch()

	log.Printf("Listening at %s", WADDELL_ADDR)
	// Accept connections, read message and respond
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Unable to accept: %s", err)
		} else {
			go func() {
				framed := framed.NewFramed(conn)
				defer framed.Close()
				if frame, err := framed.ReadInitial(); err != nil {
					log.Printf("Unable to start reading: %s", err)
				} else {
					for {
						if msg, err := newMessage(frame); err != nil {
							log.Printf("Unable to parse initial message: %s", err)
						} else {
							messagesIn <- &MessageWithConn{msg: msg, conn: framed}
						}
						frame, err = frame.Next()
						if err == io.EOF {
							return
						} else if err != nil {
							log.Printf("Unable to read next message: %s", err)
						}
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
					msgTo:                 make(chan *Message, 1),
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
					msgTo:                 make(chan *Message, 1),
				}
				go to.dispatch()
				clients[out.to] = to
			}
			if to.conn != nil {
				to.msgTo <- out
			} else {
				log.Printf("Tried to send to disconnected recipient: %s", to.addr)
				// Discard the frame to make sure that reading can continue
				out.frame.Discard()
			}
		}
	}
}

func (client *Client) dispatch() {
	for {
		select {
		case out := <-client.msgFrom:
			switch out.op {
			case OP_APPROVE:
				client.subscriptionApprovals[out.to] = true
				if client.subscriptionRequests[out.to] {
					client.subscriptions[out.to] = true
				}
			case OP_SUBSCRIBE:
				messagesOut <- out
			case OP_SEND:
				messagesOut <- out
			case OP_PUBLISH:
				// TODO: handle publishing
				// for to, _ := range client.subscriptions {
				// 	messagesOut <- out.withTo(to)
				// }
			}
		case in := <-client.msgTo:
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
					if err := in.streamTo(client.conn); err != nil {
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

func newMessage(frame *framed.Frame) (msg *Message, err error) {
	// TODO: use buffer pool for this
	msg = &Message{frame: frame}

	// Read from, to and op from frame header
	header := frame.Header()
	if err = binary.Read(header, endianness, &msg.from); err != nil {
		return
	}
	if err = binary.Read(header, endianness, &msg.to); err != nil {
		return
	}
	err = binary.Read(header, endianness, &msg.op)

	return
}

func (msg *Message) streamTo(out *framed.Framed) error {
	if err := msg.writeHeaderTo(out, msg.frame.BodyLength()); err != nil {
		return err
	}

	// Write body
	_, err := io.Copy(out, msg.frame.Body())
	return err
}

func (msg *Message) writeHeaderTo(out *framed.Framed, bodyLength uint16) error {
	out.WriteHeader(FRAMED_HEADER_LENGTH, bodyLength)

	// Write from, to and op to frame
	if err := binary.Write(out, endianness, &msg.from); err != nil {
		return err
	}
	if err := binary.Write(out, endianness, &msg.to); err != nil {
		return err
	}
	return binary.Write(out, endianness, &msg.op)
}

func (msg *Message) withTo(to Addr) *Message {
	return &Message{
		from: msg.from,
		to:   to,
		op:   OP_SEND,
	}
}
