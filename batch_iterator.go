package asc

import (
	"fmt"
	"github.com/aerospike/aerospike-client-go"
	"github.com/viant/toolbox"
	"os"
	"sync"
	"sync/atomic"
)

//BatchIterator represents a helper iterator for full scan, in this case keys were scanned first from each node separately
//to be used by this batch iterator, after keys files are fully process they will be removed.
type BatchIterator struct {
	mutex        *sync.RWMutex
	fileNames    []string
	fileIndex    int32
	namespace    string
	table        string
	binNames     []string
	client       *aerospike.Client
	batchPolicy  *aerospike.BatchPolicy
	batch        *Batch
	fileInfo     os.FileInfo
	recordCount  int32
	filePosition int32
	err          error
	batchSize    int
	file         *os.File
}

//initialiseScannerIfNeeded, returns true if success
func (i *BatchIterator) initialiseScannerIfNeeded(filename string) bool {
	if i.file != nil {
		return true
	}
	i.file, i.err = os.Open(filename)
	if i.err != nil {
		return false
	}
	i.fileInfo, i.err = i.file.Stat()
	if i.err != nil {
		return false
	}
	return true
}

func (i *BatchIterator) scanKeys() ([]*aerospike.Key, bool) {
	var keys = make([]*aerospike.Key, 0)
	for j := 0; j < i.batchSize; j++ {

		var position = int(atomic.LoadInt32(&i.filePosition))
		if position >= int(i.fileInfo.Size()) {

			i.file.Close()
			atomic.StoreInt32(&i.filePosition, 0)
			i.file = nil
			i.fileInfo = nil
			toolbox.RemoveFileIfExist(i.fileNames[i.fileIndex])
			atomic.AddInt32(&i.fileIndex, 1)
			atomic.StoreInt32(&i.recordCount, 0)
			break
		}

		key, readCount, err := ReadKey(i.file, i.namespace, i.table)
		atomic.AddInt32(&i.filePosition, int32(readCount))
		if err != nil {
			i.err = err
			return nil, false
		}
		keys = append(keys, key)
	}
	return keys, true
}

func (i *BatchIterator) readInBatch(keys []*aerospike.Key) bool {
	i.batch.Keys = keys
	var err error
	i.batch.Records, err = i.client.BatchGet(i.batchPolicy, keys, i.binNames...)
	if err != nil {
		i.err = nil
	} else {
		atomic.AddInt32(&i.recordCount, int32(len(i.batch.Records)))
	}
	return i.err == nil
}

//HasNext check is has more record, if needed it will scan keys from files to batch corresponding records
func (i *BatchIterator) HasNext() bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	var iterationCont = 10000 //some high number
	for k := 0; k < iterationCont; k++ {
		var index = int(atomic.LoadInt32(&i.fileIndex))
		if index >= len(i.fileNames) {
			return false
		}
		if !i.initialiseScannerIfNeeded(i.fileNames[index]) {
			return true //to catch error
		}

		keys, shallContinue := i.scanKeys()
		if !shallContinue {
			return true //to catch error
		}

		if len(keys) == 0 {
			continue
		}
		if !i.readInBatch(keys) {
			return true //to catch error
		}
		if len(i.batch.Records) == 0 {
			continue
		}
		return true

	}
	return false

}

func (i *BatchIterator) Next(target interface{}) error {
	if i.err != nil {
		return i.err
	}
	if targetPointer, ok := target.(*Batch); ok {
		*targetPointer = *i.batch
		return nil
	}
	if targetPointer, ok := target.(**Batch); ok {
		*targetPointer = i.batch
		return nil
	}
	return fmt.Errorf("unsupporter target type %T, expected %T or %T", target, i.batch, &i.batch)
}

func NewBatchIterator(client *aerospike.Client, batchPolicy *aerospike.BatchPolicy, batchSize int, namespace, table string, fileNames []string, binNames ...string) *BatchIterator {
	return &BatchIterator{
		mutex:       &sync.RWMutex{},
		client:      client,
		batchPolicy: batchPolicy,
		batchSize:   batchSize,
		namespace:   namespace,
		table:       table,
		fileNames:   fileNames,
		binNames:    binNames,
		batch:       &Batch{},
	}

}

type Batch struct {
	Keys    []*aerospike.Key
	Records []*aerospike.Record
}
