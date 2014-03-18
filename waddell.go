package main

import (
	"encoding/binary"
	"github.com/oxtoacart/ftcp"
	"io"
	"log"
	"runtime"
)

type Op uint16

const (
	OP_SUBSCRIBE = Op(1)
	OP_APPROVE   = Op(2)
	OP_SEND      = Op(3)
	OP_PUBLISH   = Op(4)
	WADDELL_ADDR = "127.0.0.1:10080"
)

// Client represents a client of waddell's
type Client struct {
	addr                  string
	conn                  *ftcp.Conn
	subscriptionRequests  map[string]bool // requests to subscribe to Client's messages
	subscriptionApprovals map[string]bool // permission to subscribe to Client's messages
	subscriptions         map[string]bool
	msgFrom               chan *Message
	msgTo                 chan *Message
}

type Message struct {
	sender    string
	recipient string
	op        Op
	data      []byte
}

type MessageWithConn struct {
	msg  *Message
	conn *ftcp.Conn
}

var (
	clients     = make(map[string]*Client)
	messagesIn  = make(chan *MessageWithConn, 100000)
	messagesOut = make(chan *Message, 100000)
)

func main() {
	runtime.GOMAXPROCS(2)
	listener, err := ftcp.Listen(WADDELL_ADDR)
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
				reader := conn.Reader()
				defer reader.Close()
				for {
					if msg, err := reader.Read(); err == nil {
						messagesIn <- &MessageWithConn{newMessage(msg), conn}
					} else {
						if err == io.EOF {
							return
						} else {
							log.Printf("Unable to read message: %s", err)
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
			sender := clients[in.msg.sender]
			if sender == nil {
				sender = &Client{
					addr:                  in.msg.sender,
					subscriptionRequests:  make(map[string]bool),
					subscriptionApprovals: make(map[string]bool),
					subscriptions:         make(map[string]bool),
					msgFrom:               make(chan *Message, 100),
					msgTo:                 make(chan *Message, 100),
				}
				go sender.dispatch()
				clients[in.msg.sender] = sender
			}
			sender.conn = in.conn
			sender.msgFrom <- in.msg
		case out := <-messagesOut:
			recipient := clients[out.recipient]
			if recipient == nil {
				recipient = &Client{
					addr:                  out.recipient,
					subscriptionRequests:  make(map[string]bool),
					subscriptionApprovals: make(map[string]bool),
					subscriptions:         make(map[string]bool),
					msgFrom:               make(chan *Message, 100),
					msgTo:                 make(chan *Message, 100),
				}
				go recipient.dispatch()
				clients[out.recipient] = recipient
			}
			if recipient.conn != nil {
				recipient.msgTo <- out
			} else {
				log.Printf("Tried to send to disconnected recipient: %s", recipient.addr)
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
				client.subscriptionApprovals[out.recipient] = true
				if client.subscriptionRequests[out.recipient] {
					client.subscriptions[out.recipient] = true
				}
			case OP_SUBSCRIBE:
				messagesOut <- out
			case OP_SEND:
				messagesOut <- out
			case OP_PUBLISH:
				for recipient, _ := range client.subscriptions {
					messagesOut <- out.to(recipient)
				}
			}
		case in := <-client.msgTo:
			switch in.op {
			case OP_APPROVE:
				log.Printf("Got request to approve subscription from a different user - this shouldn't happen")
			case OP_SUBSCRIBE:
				client.subscriptionRequests[in.sender] = true
				if client.subscriptionApprovals[in.sender] {
					// Complete subscription
					client.subscriptions[in.sender] = true
				}
			case OP_SEND:
				if client.conn != nil { // TODO: make sure sender is actually approved
					if err := client.conn.Write(encodeMessage(in)); err != nil {
						if err == io.EOF {
							client.conn = nil
						} else {
							log.Printf("Unable to send message from %s to %s: %s", in.sender, in.recipient, err)
						}
					}
				} else if client.conn == nil {
					log.Printf("Client %s has no connection", client.addr)
				} else {
					log.Printf("%s is not approved to send to %s", in.sender, in.recipient)
				}
			}
		}
	}
}

func newMessage(ftcpMsg *ftcp.Message) (msg *Message) {
	// Read sender and recipient from message
	// The sender and recipient are variable length, with the first pair
	// of 16 bit integers indicating their relative lengths
	senderLength := binary.BigEndian.Uint16(ftcpMsg.Data[0:2])
	recipientLength := binary.BigEndian.Uint16(ftcpMsg.Data[2:4])

	senderStart := 4
	senderEnd := senderStart + int(senderLength)
	recipientStart := senderEnd
	recipientEnd := recipientStart + int(recipientLength)
	opStart := recipientEnd
	opEnd := opStart + 2
	dataStart := opEnd

	sender := string(ftcpMsg.Data[senderStart:senderEnd])
	recipient := string(ftcpMsg.Data[recipientStart:recipientEnd])
	op := Op(binary.BigEndian.Uint16(ftcpMsg.Data[opStart:opEnd]))

	msg = &Message{
		sender:    sender,
		recipient: recipient,
		op:        op,
		data:      ftcpMsg.Data[dataStart:],
	}

	return
}

func encodeMessage(msg *Message) *ftcp.Message {
	senderLength := len(msg.sender)
	recipientLength := len(msg.recipient)
	dataLength := len(msg.data)

	senderStart := 4
	senderEnd := senderStart + senderLength
	recipientStart := senderEnd
	recipientEnd := recipientStart + recipientLength
	opStart := recipientEnd
	opEnd := opStart + 2
	dataStart := opEnd

	bytes := make([]byte, 6+senderLength+recipientLength+dataLength)
	binary.BigEndian.PutUint16(bytes[0:2], uint16(senderLength))
	binary.BigEndian.PutUint16(bytes[2:4], uint16(recipientLength))
	binary.BigEndian.PutUint16(bytes[opStart:opEnd], uint16(msg.op))
	senderBytes := []byte(msg.sender)
	recipientBytes := []byte(msg.recipient)
	for i := 0; i < senderLength; i++ {
		bytes[senderStart+i] = senderBytes[i]
	}
	for i := 0; i < recipientLength; i++ {
		bytes[recipientStart+i] = recipientBytes[i]
	}
	for i := 0; i < dataLength; i++ {
		bytes[dataStart+i] = msg.data[i]
	}

	return &ftcp.Message{Data: bytes}
}

func (msg *Message) to(recipient string) *Message {
	return &Message{
		sender:    msg.sender,
		recipient: recipient,
		op:        OP_SEND,
		data:      msg.data,
	}
}
