package main

import (
	"io"
)

// Message captures all information for a message sent through Waddell
type Message struct {
	pool   *MessagePool
	conn   io.ReadWriteCloser
	buffer []byte
	from   Addr
	to     Addr
	op     Op
	n      int // number of bytes buffered from last read
}

/*
MessagePool implements a leaky pool of Messages in the form of a bounded
channel.
*/
type MessagePool struct {
	c chan *Message
	w int
}

/*
NewMessagePool creates a new MessagePool bounded to the given maxSize, with new body
byte arrays sized based on width.
*/
func NewMessagePool(maxSize int, width int) *MessagePool {
	return &MessagePool{
		c: make(chan *Message, maxSize),
		w: width,
	}
}

/*
Get gets a *Message from the MessagePool, or creates a new one if none are available
in the pool.
*/
func (p *MessagePool) Get() (m *Message) {
	select {
	case m = <-p.c:
		// reuse existing message, zero out headers
		m.from = Addr(0)
		m.to = Addr(0)
		m.op = Op(0)
	default:
		// create new message
		m = &Message{
			pool:   p,
			conn:   nil,
			buffer: make([]byte, p.w),
		}
	}
	return
}

/*
Put returns the given Message to the MessagePool.
*/
func (p *MessagePool) Put(m *Message) {
	select {
	case p.c <- m:
		// message went back into pool
	default:
		// message didn't go back into pool, just discard
	}
}

func (m *Message) ReadFrom(conn io.ReadWriteCloser) (err error) {
	m.n, err = conn.Read(m.buffer)
	if err != nil {
		return
	}
	m.from = Addr(endianness.Uint64(m.buffer))
	m.to = Addr(endianness.Uint64(m.buffer[8:16]))
	m.op = Op(endianness.Uint16(m.buffer[16:18]))
	m.conn = conn
	return
}

func (m *Message) WriteTo(w io.Writer) error {
	endianness.PutUint64(m.buffer, uint64(m.from))
	endianness.PutUint64(m.buffer[8:16], uint64(m.to))
	endianness.PutUint16(m.buffer[16:18], uint16(m.op))
	_, err := w.Write(m.buffer[:m.n])
	return err
}

func (m *Message) Body() []byte {
	return m.buffer[18:m.n]
}

func (m *Message) Set(from Addr, to Addr, op Op, body []byte) {
	m.from = from
	m.to = to
	m.op = op
	m.n = 18
	if body != nil {
		var i int
		for i = 0; i < len(body); i++ {
			m.buffer[i+18] = body[i]
		}
		m.n += len(body)
	}
}

/*
Release releases a Message by putting it back in its pool.
*/
func (m *Message) Release() {
	m.pool.Put(m)
}
