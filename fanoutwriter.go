// Package fanoutwriter provides the FanoutWriter structure, which is a buffer
// which may be written to and subsequently read from multiple variable speed
// readers.
package fanoutwriter

import (
	"io"
	"sync"
)

type client struct {
	fw  *FanoutWriter
	off int
}

// FanoutWriter is a io.WriteCloser which can spawn multiple io.ReadClosers
// that read at different speeds.
type FanoutWriter struct {
	sync.Mutex
	update  *sync.Cond
	buf     []byte
	off     int
	clients map[*client]struct{}
	closed  bool
}

// NewFanoutWriter creates a new FanoutWriter with no initial data
func NewFanoutWriter() *FanoutWriter {
	return NewFanoutWriterWithBuffer(make([]byte, 0, 0))
}

// NewFanoutWriterWithBuffer creates a new FanoutWriter with the supplied initial
// buffer which may contain data
func NewFanoutWriterWithBuffer(buf []byte) *FanoutWriter {
	f := &FanoutWriter{
		buf:     buf,
		off:     0,
		clients: make(map[*client]struct{}),
		closed:  false,
	}
	f.update = sync.NewCond(f)
	return f
}

// Write implements the standard Write interface: it writes data to the
// internal buffer, which will be read by all readers which were created before
// the call. Write only returns an error when it was previously closed.
func (f *FanoutWriter) Write(p []byte) (n int, err error) {
	blen := len(p)
	if blen == 0 {
		return 0, nil
	}

	f.Lock()

	if f.closed {
		f.Unlock()
		return 0, io.ErrClosedPipe
	}

	if len(f.clients) == 0 {
		// since new clients are created at the head of the buffer,
		// when there are no clients we throw away the data
		f.Unlock()
		return blen, nil
	}

	f.buf = append(f.buf, p...)

	// notify any waiting clients
	f.update.Broadcast()

	f.Unlock()
	return blen, nil
}

// Write closes the FanoutWriter, causing the remaining buffer to be read by
// currently created Readers, then respond to future read requests with io.EOF.
func (f *FanoutWriter) Close() error {
	f.Lock()
	f.closed = true

	// tell the waiting clients that we have no more data
	f.update.Broadcast()
	f.Unlock()
	return nil
}

// must be called while f is locked
func (f *FanoutWriter) updateOff() {
	offJump := 0
	for c, _ := range f.clients {
		offDiff := c.off - f.off

		if offDiff > offJump {
			offJump = offDiff
		}
	}

	f.buf = f.buf[offJump:]
	f.off += offJump
}

// Reader creates a new reader pointed at the end of the current buffer. Reader
// will be able to read any data written to the FanoutWriter after the reader
// is created until either the Reader or Writer is closed.
func (f *FanoutWriter) Reader() (r io.ReadCloser) {
	f.Lock()
	if f.closed {
		panic("FanoutWriter: attempted to create a new Reader when closed.")
	}
	r = &client{
		fw:  f,
		off: f.off + len(f.buf),
	}
	f.Unlock()
	return
}

// Read implements the standard Read interface: it reads data which is
// available, blocking if there is no data available. If the Writer was closed,
// Read will first return all remaining data in the buffer, then on subsequent
// reads return an error of io.EOF.
func (c *client) Read(p []byte) (n int, err error) {
	c.fw.Lock()
	for {
		localoff := c.off - c.fw.off
		lbuf := c.fw.buf[localoff:]
		// regardless of whether or not we have any space to read, we need to
		// check if the writer has any more data and has closed
		if len(lbuf) == 0 {
			if c.fw.closed {
				c.fw.Unlock()
				return 0, io.EOF
			} else {
				// we need to wait for more data
				c.fw.update.Wait()
				continue
			}
		}

		ncopy := copy(p, lbuf)
		if ncopy > 0 {
			c.off += ncopy
			c.fw.updateOff()
		}

		c.fw.Unlock()
		return ncopy, nil
	}
}

// Close closes the reader. Readers should always be closed, as it allows for
// data not yet read to be freed.
func (c *client) Close() error {
	c.fw.Lock()
	delete(c.fw.clients, c)
	// if we are the current offset, maybe we are the farthest behind and thus
	// we need to release unneeded bytes
	if c.fw.off == c.off {
		c.fw.updateOff()
	}
	c.fw.Unlock()
	return nil
}
