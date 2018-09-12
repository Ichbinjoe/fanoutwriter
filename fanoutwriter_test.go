package fanoutwriter

import (
	"github.com/stretchr/testify/assert"
	"io"
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

func TestCreateReaderWriteThenRead(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	r := fw.NewReader()
	r2 := fw.NewReader()

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
	wb := []byte{1, 2, 3, 4, 5}
	validateWrite(t, fw, wb)

	r := fw.NewReader()

	validateWrite(t, fw, wb)
	rb := make([]byte, 10, 10)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, wb, rb[:5])
}

func TestWriteCreateReaderWriteThenReadWithReadFromStart(t *testing.T) {
	fw := NewFanoutWriter(&FanoutWriterConfig{
		ReadFromStart: true,
	})
	wb := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	validateWrite(t, fw, wb[:5])

	r := fw.NewReader()

	validateWrite(t, fw, wb[5:])
	rb := make([]byte, 10, 10)

	validateRead(t, r, rb, 10)

	assert.ElementsMatch(t, wb, rb)
}
