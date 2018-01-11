package asc

import (
	"bytes"
	"fmt"
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/toolbox"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

const bufferSize = 1024 * 1024 * 256

type KeyScanner struct {
	client        *aerospike.Client
	scanPolicy    *aerospike.ScanPolicy
	baseDirectory string
	namespace     string
	dataSet       string
	writer        *bufferedWriter
}

func (s *KeyScanner) scanAll() ([]string, error) {

	filename := path.Join(s.baseDirectory, fmt.Sprintf("%v-%v.keys", s.namespace, s.dataSet))
	toolbox.RemoveFileIfExist(filename)

	writer, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	s.writer = newBufferedWriter(writer, bufferSize)
	defer s.writer.Close()
	var result = []string{filename}
	recordSet, err := s.client.ScanAll(s.scanPolicy, s.namespace, s.dataSet)
	if err != nil {
		return nil, err
	}
	records := recordSet.Records
	errors := recordSet.Errors
	for recordSet.IsActive() {
		select {
		case record := <-records:
			if record != nil && record.Key != nil {
				err = WriteKey(record.Key, s.writer)
				if err != nil {
					return nil, err
				}
			}
		case err = <-errors:
			if err != nil {
				log.Printf("failed to scan keys: %v", err)
				return nil, err
			}
		}
	}
	return result, nil
}

func (s *KeyScanner) Scan() ([]string, error) {
	return s.scanAll()
}

func NewKeyScanner(client *aerospike.Client, scanPolicy *aerospike.ScanPolicy, baseDirectory, namespace, dataSet string) *KeyScanner {
	return &KeyScanner{
		client:        client,
		scanPolicy:    scanPolicy,
		baseDirectory: baseDirectory,
		namespace:     namespace,
		dataSet:       dataSet,
	}
}

//writer tries to hold in memory keys
type bufferedWriter struct {
	size       int32
	bufSize    int
	flushCount int32
	buf        *bytes.Buffer
	writer     io.WriteCloser
	mux        *sync.Mutex
}

func (w *bufferedWriter) flush() error {
	var data = w.buf.Bytes()
	written, err := w.writer.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return fmt.Errorf("wrote %v of %v", written, len(data))
	}
	w.buf.Reset()
	atomic.AddInt32(&w.flushCount, 1)
	atomic.StoreInt32(&w.size, 0)
	return nil
}

func (w *bufferedWriter) pendingSize() int {
	return int(atomic.LoadInt32(&w.size))
}

func (w *bufferedWriter) Write(p []byte) (n int, err error) {
	w.mux.Lock()
	defer w.mux.Unlock()
	if w.pendingSize()+len(p) > w.bufSize {
		err = w.flush()
		if err != nil {
			return 0, err
		}
	}
	atomic.AddInt32(&w.size, int32(len(p)))
	return w.buf.Write(p)
}

func (w *bufferedWriter) Close() (err error) {
	if w.pendingSize() > 0 {
		err = w.flush()
		if err != nil {
			return err
		}
	}
	return w.writer.Close()
}

func newBufferedWriter(writer io.WriteCloser, bufSize int) *bufferedWriter {
	return &bufferedWriter{
		bufSize: bufSize,
		buf:     new(bytes.Buffer),
		writer:  writer,
		mux:     &sync.Mutex{},
	}
}
