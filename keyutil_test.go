package asc_test

import (
	"bytes"
	"github.com/aerospike/aerospike-client-go"
	"github.com/stretchr/testify/assert"
	"github.com/viant/asc"
	"testing"
)

func Test_WriteKey(t *testing.T) {

	var keys = []interface{}{
		12346,
		"testKey1 42",
		int64(91827364),
		uint32(1364),
		[]byte{0x1, 0x1, 0x3},
	}

	buf := new(bytes.Buffer)

	for _, value := range keys {
		key, err := aerospike.NewKey("a", "b", value)
		assert.Nil(t, err)
		err = asc.WriteKey(key, buf)
		assert.Nil(t, err)
	}

	reader := bytes.NewReader(buf.Bytes())
	for _, value := range keys {
		key, _, err := asc.ReadKey(reader, "a", "b")
		assert.Nil(t, err)
		assert.EqualValues(t, key.Value().GetObject(), value)

	}

}
