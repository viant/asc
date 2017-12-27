package asc

import (
	"encoding/binary"
	"fmt"
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/toolbox"
	"io"
)

const (
	keyTypeDigest = 0x0
	keyTypeInt    = 0x1
	keyTypeString = 0x2
	keyTypeBytes  = 0x3
)

func WriteKey(key *aerospike.Key, writer io.Writer) (err error) {
	var source = key.Value()
	var payload []byte
	switch value := source.GetObject().(type) {
	case string:
		textLength := uint8(len(value))
		payload = append([]byte{keyTypeString, textLength}, []byte(value)...)
	case []byte:
		textLength := uint8(len(value))
		payload = append([]byte{keyTypeBytes, textLength}, []byte(value)...)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		var data = make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(toolbox.AsInt(value)))
		payload = append([]byte{keyTypeInt}, data...)

	default:
		payload = append([]byte{keyTypeDigest}, key.Digest()...)
	}
	n, err := writer.Write(payload)
	if err != nil {
		return err
	}
	if n != len(payload) {
		return fmt.Errorf("failed to write: %v bytes, written only: %v", len(payload), n)
	}
	return nil
}

func readNBytes(reader io.Reader, bytesCount int) ([]byte, error) {
	data := make([]byte, bytesCount)
	n, err := reader.Read(data)
	if err != nil {
		return nil, err
	}
	if n != bytesCount {
		return nil, fmt.Errorf("failed to read: %v bytes, read only: %v", bytesCount, n)
	}
	return data, nil
}

func ReadKey(reader io.Reader, namespace, setName string) (*aerospike.Key, int, error) {
	keyType, err := readNBytes(reader, 1)
	if err != nil {
		return nil, 0, err
	}
	switch keyType[0] {
	case keyTypeDigest:
		payload, err := readNBytes(reader, 20)
		if err != nil {
			return nil, 0, err
		}
		key, err := aerospike.NewKeyWithDigest(namespace, setName, nil, payload)
		return key, 21, err
	case keyTypeString:
		dataSize, err := readNBytes(reader, 1)
		if err != nil {
			return nil, 0, err
		}
		payload, err := readNBytes(reader, int(uint(dataSize[0])))
		if err != nil {
			return nil, 0, err
		}
		key, err := aerospike.NewKey(namespace, setName, string(payload))
		return key, 2 + len(payload), err
	case keyTypeBytes:
		dataSize, err := readNBytes(reader, 1)
		if err != nil {
			return nil, 0, err
		}
		payload, err := readNBytes(reader, int(uint(dataSize[0])))
		if err != nil {
			return nil, 0, err
		}
		key, err := aerospike.NewKey(namespace, setName, payload)
		return key, 2 + len(payload), err
	case keyTypeInt:
		payload, err := readNBytes(reader, 8)
		if err != nil {
			return nil, 0, err
		}
		keyValue := binary.LittleEndian.Uint64(payload)
		key, err := aerospike.NewKey(namespace, setName, int(keyValue))
		return key, 9, err
	}
	return nil, 0, fmt.Errorf("unsupported key type: %v", keyTypeInt)
}
