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
	assert.Equal(t, n, l, "length read unexpected")
}

func TestCreateReaderWriteThenRead(t *testing.T) {
	fw := NewDefaultFanoutWriter()
	r := fw.NewReader()

	wb := []byte{1, 2, 3, 4, 5}

	validateWrite(t, fw, wb)

	rb := make([]byte, 5, 5)

	validateRead(t, r, rb, 5)

	assert.ElementsMatch(t, wb, rb)
}
