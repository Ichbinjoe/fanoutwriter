// Package fanoutwriter provides the FanoutWriter structure, which is a buffer
// which may be written to and subsequently read from multiple variable speed
// readers.
package fanoutwriter

import (
	"errors"
	"io"
	"sync"
)

var (
	ErrFellBehind = errors.New("reader fell behind the writers buffer limit")
)

// FanoutWriter is a io.WriteCloser which can spawn multiple io.ReadClosers
// that read at different speeds.
type FanoutWriter interface {
	io.WriteCloser
	NewReader() io.ReadCloser // Returns a new reader which begins reading depending on the configuration
}

type client struct {
	fw  *fwriter
	off int
}

type fwriter struct {
	sync.Mutex
	buf     []byte
	update  *sync.Cond
	c       *FanoutWriterConfig
	off     int
	clients map[*client]struct{}
	closed  bool
}

type FanoutWriterConfig struct {
	Buf           []byte // Initial buffer of the writer.
	Limit         int    // Limit for the size of which buffer may grow
	ReadFromStart bool   // Whether or not to start a reader from the end or beginning of the buffer.
}

// NewDefaultFanoutWriter creates a new FanoutWriter with no initial data and
// with no buffer limit.
func NewDefaultFanoutWriter() FanoutWriter {
	return NewFanoutWriter(&FanoutWriterConfig{
		Buf:           nil,
		Limit:         0,
		ReadFromStart: false,
	})
}

// NewFanoutWriter creates a new FanoutWriter with the configuration passed.
func NewFanoutWriter(c *FanoutWriterConfig) FanoutWriter {
	off := 0
	if !c.ReadFromStart {
		off = len(c.Buf)
	}

	f := &fwriter{
		buf:     c.Buf,
		c:       c,
		off:     off,
		clients: make(map[*client]struct{}),
		closed:  false,
	}

	f.update = sync.NewCond(f)
	return f
}

// Write implements the standard Write interface: it writes data to the
// internal buffer, which will be read by all readers which were created before
// the call (unless ReadFromStart is true). Write only returns an error when it
// was previously closed.
func (f *fwriter) Write(p []byte) (n int, err error) {
	blen := len(p)
	if blen == 0 {
		return 0, nil
	}

	f.Lock()

	if f.closed {
		f.Unlock()
		return 0, io.ErrClosedPipe
	}

	if !f.c.ReadFromStart && len(f.clients) == 0 {
		// since new clients are created at the head of the buffer,
		// when there are no clients we throw away the data
		f.Unlock()
		return blen, nil
	}

	if f.c.Limit != 0 {
		if f.c.Limit > blen {
			// figure out how many bytes are pushed off the end
			invalidBytes := len(f.buf) + blen - f.c.Limit
			if invalidBytes > 0 {
				// chop those bytes off
				f.buf = append(f.buf[invalidBytes:], p...)
				// move the offset pointer forward
				f.off += invalidBytes
			} else {
				// we can fit all of blen into the buffer
				f.buf = append(f.buf, p...)
			}
		} else {
			// we need to invalidate ALL of f.buf since we will be replacing
			// all of it
			f.off += len(f.buf)
			f.buf = p[len(p)-f.c.Limit:]
		}
	} else {
		// since there is no limiting factor that doesn't panic, off will never
		// update
		f.buf = append(f.buf, p...)
	}

	// notify any waiting clients
	f.update.Broadcast()

	f.Unlock()
	return blen, nil
}

// Write closes the FanoutWriter, causing the remaining buffer to be read by
// currently created Readers, then respond to future read requests with io.EOF.
func (f *fwriter) Close() error {
	f.Lock()
	f.closed = true

	// tell the waiting clients that we have no more data
	f.update.Broadcast()
	f.Unlock()
	return nil
}

// must be called while f is locked
func (f *fwriter) updateOff() {
	// so if we are ReadingFromStart, we let Limit during Write handle clipping
	// old data off. Otherwise, we handle it here.
	if !f.c.ReadFromStart {
		offJump := len(f.buf)
		for c, _ := range f.clients {
			offDiff := c.off - f.off

			if offDiff < offJump {
				offJump = offDiff
			}
		}

		f.buf = f.buf[offJump:]
		f.off += offJump
	}
}

// Reader creates a new reader pointed at the end of the current buffer. Reader
// will be able to read any data written to the FanoutWriter after the reader
// is created until either the Reader or Writer is closed.
func (f *fwriter) NewReader() (r io.ReadCloser) {
	f.Lock()
	if f.closed {
		panic("FanoutWriter: attempted to create a new Reader when closed.")
	}

	off := f.off
	if !f.c.ReadFromStart {
		off += len(f.buf)
	}

	c := &client{
		fw:  f,
		off: off,
	}
	r = c

	f.clients[c] = struct{}{}

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

		// first, lets detect whether we 'fell off the end' (due to a limit
		// constraint). This is an error state, so we need to report it.
		if localoff > len(c.fw.buf) || localoff < 0 {
			// if our offset minus their offset is greater then len, then we
			// could have NEVER gotten this offset UNLESS the writer offset has
			// surpassed us.
			// At this point, we consider this reader to be 'closed'.
			delete(c.fw.clients, c)
			c.fw.Unlock()
			return 0, ErrFellBehind
		}

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
