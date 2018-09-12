package fanoutwriter

import (
	"github.com/stretchr/testify/assert"
	"io"
	"runtime"
	"sync"
	"testing"
)

func validateWrite(t *testing.T, f io.Writer, b []byte) {
	n, err := f.Write(b)
	assert.NoError(t, err, "Write should not return with an error")
	assert.Equal(t, len(b), n, "length written should match buffer length")
}

func validateRead(t *testing.T, r io.Reader, b []byte, l int) {
	n, err := r.Read(b)
	assert.NoError(t, err, "Read should not return with an error")
	assert.Equal(t, l, n, "length read unexpected")
}

func vclose(t *testing.T, c io.Closer) {
	e := c.Close()
	assert.NoError(t, e, "Close should not return with an error")
}

func TestCreateReaderWriteThenRead(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	r := fw.NewReader()
	r2 := fw.NewReader()

	defer vclose(t, fw)
	defer vclose(t, r)
	defer vclose(t, r2)

	wb := []byte{1, 2, 3, 4, 5}

	validateWrite(t, fw, wb)

	rb := make([]byte, 5, 5)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, wb, rb)

	validateRead(t, r2, rb, 5)
	assert.ElementsMatch(t, wb, rb)
}

func TestWriteCreateReaderWriteThenRead(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	defer vclose(t, fw)

	wb := []byte{1, 2, 3, 4, 5}
	validateWrite(t, fw, wb)

	r := fw.NewReader()
	defer vclose(t, r)

	validateWrite(t, fw, wb)
	rb := make([]byte, 10, 10)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, wb, rb[:5])
}

func TestWriteCreateReaderWriteThenReadWithReadFromStart(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		ReadFromStart: true,
	})
	defer vclose(t, fw)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb[:5])

	r := fw.NewReader()
	defer vclose(t, r)

	validateWrite(t, fw, wb[5:])
	rb := make([]byte, 10, 10)

	validateRead(t, r, rb, 10)

	assert.ElementsMatch(t, wb, rb)
}

func TestRFSLimitDoubleWriteOverCreateReaderRead(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Limit:         6,
		ReadFromStart: true,
	})
	defer vclose(t, fw)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb[:5])

	validateWrite(t, fw, wb[5:])
	rb := make([]byte, 10, 10)

	r := fw.NewReader()
	defer vclose(t, r)
	validateRead(t, r, rb, 6)

	// rb should contain the last 6 elements of wb
	assert.ElementsMatch(t, wb[4:], rb[:6])
}

func TestRFSLimitWriteOverCreateReaderRead(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Limit:         6,
		ReadFromStart: true,
	})

	defer vclose(t, fw)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb)

	rb := make([]byte, 10, 10)

	r := fw.NewReader()
	defer vclose(t, r)

	validateRead(t, r, rb, 6)

	// rb should contain the last 6 elements of wb
	assert.ElementsMatch(t, wb[4:], rb[:6])
}

func TestInitialBuffer(t *testing.T) {
	b := []byte{1, 2, 3, 4, 5}
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Buf:           b,
		ReadFromStart: true,
	})

	defer vclose(t, fw)

	rb := make([]byte, 10, 10)

	r := fw.NewReader()
	defer vclose(t, r)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, b, rb[:5])
}

func TestInitialBufferCreateReaderWriteRead(t *testing.T) {
	b := []byte{1, 2, 3, 4, 5}
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Buf:           b[:3],
		ReadFromStart: true,
	})

	defer vclose(t, fw)

	r := fw.NewReader()
	defer vclose(t, r)

	// so like make sure we actually copy things in i guess
	b[3] = 6
	b[4] = 7
	validateWrite(t, fw, b[3:])

	rb := make([]byte, 10, 10)
	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, b, rb[:5])
}

func TestZeroSizeWrite(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	defer vclose(t, fw)
	r := fw.NewReader()
	defer vclose(t, r)

	wb := []byte{1, 2, 3, 4, 5}
	validateWrite(t, fw, wb[0:0])
	validateWrite(t, fw, wb)

	rb := make([]byte, 10, 10)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, wb, rb[:5])
}

func TestWriteAfterClose(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	e := fw.Close()
	assert.NoError(t, e)

	_, e = fw.Write([]byte{1, 2, 3, 4, 5})
	assert.Equal(t, io.ErrClosedPipe, e)
}

func TestCreateReaderAfterClose(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fail() // we should have paniced!
		}
	}()

	fw := NewDefaultFanoutWriter()
	e := fw.Close()
	assert.NoError(t, e)

	fw.NewReader() // uh oh!
}

func TestLimitWriteWouldCauseFallBehind(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Limit: 6,
	})
	defer vclose(t, fw)

	r := fw.NewReader()
	defer vclose(t, r)
	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	_, e := fw.Write(wb)
	assert.Equal(t, ErrWriteWouldCauseFallBehind, e)
}

func TestLimitNoReaderWriteWouldCauseFallBehindNoErr(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Limit: 6,
	})
	defer vclose(t, fw)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb)
}

func TestLimitReaderFellBehind(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		Limit: 6,
	})
	defer vclose(t, fw)
	r := fw.NewReader()
	defer vclose(t, r)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb[:5])
	validateWrite(t, fw, wb[5:])

	rb := make([]byte, 10, 10)
	_, err := r.Read(rb)

	assert.Equal(t, ErrFellBehind, err)
}

func TestDeferredWriteAndClose(t *testing.T) {
	fw := NewDefaultFanoutWriter()

	r := fw.NewReader()
	defer vclose(t, r)

	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		rb := make([]byte, 15, 15)
		validateRead(t, r, rb, 10)
		assert.ElementsMatch(t, wb, rb[:10])
		rb2 := make([]byte, 10, 10)
		_, err := r.Read(rb2)
		assert.Equal(t, io.EOF, err)
		wg.Done()
	}()

	// we should yield to the other so it can start its read call
	runtime.Gosched()

	// so we write to the writer...
	validateWrite(t, fw, wb[:10])
	// ...then close it
	e := fw.Close()
	assert.NoError(t, e)

	wg.Wait()
}
