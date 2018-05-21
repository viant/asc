package asc

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"testing"
)

func TestAsArray(t *testing.T) {
	var source = map[string]interface{}{
		"k": map[interface{}]interface{}{
			"a": 1,
			"b": 2,
		},
	}
	array, err := AsArray("k", source)
	assert.Nil(t, err)
	var expected = `[
	{
		"@indexBy@":"key"
	},
	{
		"key": "b",
		"value": 2
	},
	{
		"key": "a",
		"value": 1
	}
]`
	assertly.AssertValues(t, expected, array)
}

func TestAsJSON(t *testing.T) {
	var source = map[string]interface{}{
		"k1": []int{1, 2, 3},
		"k2": map[interface{}]interface{}{
			"a": 1,
			"b": 2,
		},
	}
	{
		JSON, err := AsJSON("k1", source)
		assert.Nil(t, err)
		assert.Equal(t, "[\n\t1,\n\t2,\n\t3\n]", JSON)
	}
	{
		JSON, err := AsJSON("k2", source)
		assert.Nil(t, err)
		assert.Equal(t, "{\n\t\"a\": 1,\n\t\"b\": 2\n}", JSON)
	}
}
