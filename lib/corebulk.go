// Copyright 2013 Matthew Baird
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elastigo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Max buffer size in bytes before flushing to elasticsearch
	BulkMaxBuffer = 16384
	// Max number of Docs to hold in buffer before forcing flush
	BulkMaxDocs = 100
	// Max delay before forcing a flush to Elasticearch
	BulkDelaySeconds = 5
	// maximum wait shutdown seconds
	MAX_SHUTDOWN_SECS = 5
)

// ErrList is a list of errors that implement the error interface.
type ErrList []error

// Error returns the combined messages of the errors in the ErrList.
func (e ErrList) Error() string {
	var buf bytes.Buffer

	for i, err := range e {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

// syncErrList is a thread-safe error list with helper methods to
// add errors.
type syncErrList struct {
	m    sync.Mutex
	list ErrList
}

func (e *syncErrList) add(err error) {
	e.m.Lock()
	defer e.m.Unlock()
	if el, ok := err.(ErrList); ok {
		e.list = append(e.list, el...)
		return
	}
	e.list = append(e.list, err)
}

func (e *syncErrList) err() error {
	e.m.Lock()
	defer e.m.Unlock()

	if len(e.list) == 0 {
		return nil
	}
	return e.list
}

func (e *syncErrList) Error() string {
	e.m.Lock()
	defer e.m.Unlock()
	return e.list.Error()
}

// A bulk indexer creates goroutines, and channels for connecting and sending data
// to elasticsearch in bulk, using buffers.
type BulkIndexer struct {
	conn *Conn

	// We are creating a variable defining the func responsible for sending
	// to allow a mock sendor for test purposes
	Sender func(*bytes.Buffer) error

	// If we encounter an error in sending, we are going to retry for this long
	// before returning an error
	// if 0 it will not retry
	RetryForSeconds int

	// list of errors cumulated during the bulk action
	errs *syncErrList

	// channel for sending to background indexer
	bulkChannel chan []byte

	// numErrors is a running total of errors seen
	numErrors uint64

	// shutdown channel
	shutdownChan chan chan struct{}
	// channel to shutdown timer
	timerDoneChan chan bool

	// Channel to send a complete byte.Buffer to the http sendor
	sendBuf chan *bytes.Buffer
	// byte buffer for docs that have been converted to bytes, but not yet sent
	buf *bytes.Buffer
	// Buffer for Max number of time before forcing flush
	BufferDelayMax time.Duration
	// Max buffer size in bytes before flushing to elasticsearch
	BulkMaxBuffer int // 1048576
	// Max number of Docs to hold in buffer before forcing flush
	BulkMaxDocs int // 100

	// Number of documents we have send through so far on this session
	docCt int
	// Max number of http conns in flight at one time
	maxConns int
	// If we are indexing enough docs per bufferdelaymax, we won't need to do time
	// based eviction, else we do.
	needsTimeBasedFlush bool
	// Lock for document writes/operations
	mu sync.Mutex

	// wait group for http calls goros
	httpWg *sync.WaitGroup
	// wait group for doc goro
	docWg *sync.WaitGroup
}

func (b *BulkIndexer) NumErrors() uint64 {
	return atomic.LoadUint64(&b.numErrors)
}

func (c *Conn) NewBulkIndexer(maxConns int) *BulkIndexer {
	b := BulkIndexer{conn: c, sendBuf: make(chan *bytes.Buffer, maxConns)}
	b.needsTimeBasedFlush = true
	b.buf = new(bytes.Buffer)
	b.maxConns = maxConns
	b.BulkMaxBuffer = BulkMaxBuffer
	b.BulkMaxDocs = BulkMaxDocs
	b.BufferDelayMax = time.Duration(BulkDelaySeconds) * time.Second
	b.bulkChannel = make(chan []byte, 100)
	b.timerDoneChan = make(chan bool)
	b.httpWg = new(sync.WaitGroup)
	b.docWg = new(sync.WaitGroup)
	b.errs = &syncErrList{}
	return &b
}

// A bulk indexer with more control over error handling
//    @maxConns is the max number of in flight http requests
//    @retrySeconds is # of seconds to wait before retrying falied requests
func (c *Conn) NewBulkIndexerRetry(maxConns, retrySeconds int) *BulkIndexer {
	b := c.NewBulkIndexer(maxConns)
	b.RetryForSeconds = retrySeconds
	return b
}

// Starts this bulk Indexer running, this Run opens a go routine so is
// Non blocking
func (b *BulkIndexer) Start() {
	b.shutdownChan = make(chan chan struct{})

	go func() {
		// XXX(j): Refactor this stuff to use an interface.
		if b.Sender == nil {
			b.Sender = b.Send
		}
		// Backwards compatibility
		b.startHttpSender()
		b.startDocChannel()
		b.startTimer()

		ch := <-b.shutdownChan
		b.Flush()
		b.shutdown()
		close(ch)
	}()
}

// Stop stops the bulk indexer, blocking the caller until it is complete.
func (b *BulkIndexer) Stop() error {
	ch := make(chan struct{})
	b.shutdownChan <- ch
	<-ch
	close(b.shutdownChan)
	return b.errs.err()
}

func (b *BulkIndexer) PendingDocuments() int {
	return b.docCt
}

// Flush all current documents to ElasticSearch
func (b *BulkIndexer) Flush() {
	b.mu.Lock()
	if b.docCt > 0 {
		b.send(b.buf)
	}
	b.mu.Unlock()
}

func (b *BulkIndexer) startHttpSender() {
	// this sends http requests to elasticsearch it uses maxConns to open up that
	// many goroutines, each of which will synchronously call ElasticSearch
	// in theory, the whole set will cause a backup all the way to IndexBulk if
	// we have consumed all maxConns
	b.httpWg.Add(b.maxConns)
	for i := 0; i < b.maxConns; i++ {
		go func() {
			defer b.httpWg.Done()

			for buf := range b.sendBuf {
				// Copy for the potential re-send.
				bufCopy := bytes.NewBuffer(buf.Bytes())
				err := b.Sender(buf)

				// Perhaps a b.FailureStrategy(err)  ??  with different types of strategies
				//  1.  Retry, then panic
				//  2.  Retry then return error and let runner decide
				//  3.  Retry, then log to disk?   retry later?
				if err != nil {
					// check if it is worth retrying (if all errors are 4xx codes,
					// then the request is in error and won't work even with a retry).
					retry := true
					if el, ok := err.(ErrList); ok {
						// default to no retry
						retry = false
						for _, e := range el {
							if bae, ok := e.(*BulkActionError); ok {
								if bae.Status >= 500 {
									retry = true
									break
								}
							} else {
								retry = true
								break
							}
						}
					}
					if retry && b.RetryForSeconds > 0 {
						time.Sleep(time.Second * time.Duration(b.RetryForSeconds))
						err = b.Sender(bufCopy)
						if err == nil {
							// Successfully re-sent with no error
							continue
						}
					}
					b.errs.add(err)
				}
			}
		}()
	}
}

// start a timer for checking back and forcing flush ever BulkDelaySeconds seconds
// even if we haven't hit max messages/size
func (b *BulkIndexer) startTimer() {
	ticker := time.NewTicker(b.BufferDelayMax)
	go func() {
		for {
			select {
			case <-ticker.C:
				b.mu.Lock()
				// don't send unless last sendor was the time,
				// otherwise an indication of other thresholds being hit
				// where time isn't needed
				if b.buf.Len() > 0 && b.needsTimeBasedFlush {
					b.needsTimeBasedFlush = true
					b.send(b.buf)
				} else if b.buf.Len() > 0 {
					b.needsTimeBasedFlush = true
				}
				b.mu.Unlock()
			case <-b.timerDoneChan:
				// shutdown this go routine
				return
			}
		}
	}()
}

func (b *BulkIndexer) startDocChannel() {
	// This goroutine accepts incoming byte arrays from the IndexBulk function and
	// writes to buffer
	b.docWg.Add(1)
	go func() {
		defer b.docWg.Done()

		for docBytes := range b.bulkChannel {
			b.mu.Lock()
			b.docCt += 1
			b.buf.Write(docBytes)
			if b.buf.Len() >= b.BulkMaxBuffer || b.docCt >= b.BulkMaxDocs {
				b.needsTimeBasedFlush = false
				//log.Printf("Send due to size:  docs=%d  bufsize=%d", b.docCt, b.buf.Len())
				b.send(b.buf)
			}
			b.mu.Unlock()
		}
		// enqueue the remaning bytes, if any
		b.mu.Lock()
		if b.docCt > 0 {
			b.send(b.buf)
		}
		b.mu.Unlock()
	}()
}

func (b *BulkIndexer) send(buf *bytes.Buffer) {
	//b2 := *b.buf
	b.sendBuf <- buf
	b.buf = new(bytes.Buffer)
	//	b.buf.Reset()
	b.docCt = 0
}

func (b *BulkIndexer) shutdown() {
	// This must be called After flush
	close(b.bulkChannel)
	b.docWg.Wait()
	close(b.sendBuf)
	b.httpWg.Wait()
	b.timerDoneChan <- true
}

// The index bulk API adds or updates a typed JSON document to a specific index, making it searchable.
// it operates by buffering requests, and ocassionally flushing to elasticsearch
// http://www.elasticsearch.org/guide/reference/api/bulk.html
func (b *BulkIndexer) Index(index, _type, id, parent, ttl string, date *time.Time, data interface{}, refresh bool) error {
	//{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1", "parent" : "london" } }
	by, err := WriteBulkBytes("index", index, _type, id, parent, ttl, date, data, refresh)
	if err != nil {
		return err
	}
	b.bulkChannel <- by
	return nil
}

func (b *BulkIndexer) Update(index, _type, id, parent, ttl string, date *time.Time, data interface{}, refresh bool) error {
	//{ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1", "parent" : "london" } }
	by, err := WriteBulkBytes("update", index, _type, id, parent, ttl, date, data, refresh)
	if err != nil {
		return err
	}
	b.bulkChannel <- by
	return nil
}

func (b *BulkIndexer) Delete(index, _type, id string, refresh bool) {
	queryLine := fmt.Sprintf("{\"delete\":{\"_index\":%q,\"_type\":%q,\"_id\":%q,\"refresh\":%t}}\n", index, _type, id, refresh)
	b.bulkChannel <- []byte(queryLine)
	return
}

func (b *BulkIndexer) UpdateWithPartialDoc(index, _type, id, parent, ttl string, date *time.Time, partialDoc interface{}, upsert, refresh bool) error {
	var data map[string]interface{} = make(map[string]interface{})

	data["doc"] = partialDoc
	if upsert {
		data["doc_as_upsert"] = true
	}
	return b.Update(index, _type, id, parent, ttl, date, data, refresh)
}

type responseStruct struct {
	Took   int64                         `json:"took"`
	Errors bool                          `json:"errors"`
	Items  []map[string]*BulkActionError `json:"items"`
}

type BulkActionError struct {
	Action  string
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Status  int    `json:"status"`
	Message string `json:"error"`
	Index   string `json:"_index"`
}

func (b *BulkActionError) Error() string {
	return fmt.Sprintf("elastigo: %s %s/%s/%s [%d] %s",
		b.Action, b.Index, b.Type, b.ID, b.Status, b.Message)
}

// This does the actual send of a buffer, which has already been formatted
// into bytes of ES formatted bulk data
func (b *BulkIndexer) Send(buf *bytes.Buffer) error {

	body, err := b.conn.DoCommand("POST", "/_bulk", nil, buf)
	if err != nil {
		atomic.AddUint64(&b.numErrors, 1)
		return err
	}

	// check for response errors, bulk insert will give 200 OK but then include errors in response
	response := responseStruct{}
	jsonErr := json.Unmarshal(body, &response)
	if jsonErr == nil {
		if response.Errors {
			err := errorFromBulkResponse(response)
			atomic.AddUint64(&b.numErrors, uint64(len(err)))
			return err
		}
	}
	return jsonErr
}

func errorFromBulkResponse(res responseStruct) ErrList {
	var el ErrList

	for _, item := range res.Items {
		for k, v := range item {
			if v.Status >= 400 {
				v.Action = k
				el = append(el, v)
			}
		}
	}
	return el
}

// Given a set of arguments for index, type, id, data create a set of bytes that is formatted for bulkd index
// http://www.elasticsearch.org/guide/reference/api/bulk.html
func WriteBulkBytes(op, index, _type, id, parent, ttl string, date *time.Time, data interface{}, refresh bool) ([]byte, error) {
	// only index and update are currently supported
	if op != "index" && op != "update" {
		return nil, errors.New(fmt.Sprintf("Operation '%s' is not yet supported", op))
	}

	// First line
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`{"%s":{"_index":"`, op))
	buf.WriteString(index)
	buf.WriteString(`","_type":"`)
	buf.WriteString(_type)
	buf.WriteString(`"`)
	if len(id) > 0 {
		buf.WriteString(`,"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"`)
	}

	if parent != "" {
		buf.WriteString(`,"parent":"`)
		buf.WriteString(parent)
		buf.WriteString(`"`)
	}

	if op == "update" {
		buf.WriteString(`,"retry_on_conflict":3`)
	}

	if len(ttl) > 0 {
		buf.WriteString(`,"ttl":"`)
		buf.WriteString(ttl)
		buf.WriteString(`"`)
	}
	if date != nil {
		buf.WriteString(`,"_timestamp":"`)
		buf.WriteString(strconv.FormatInt(date.UnixNano()/1e6, 10))
		buf.WriteString(`"`)
	}
	if refresh {
		buf.WriteString(`,"refresh":true`)
	}
	buf.WriteString(`}}`)
	buf.WriteRune('\n')
	//buf.WriteByte('\n')
	switch v := data.(type) {
	case *bytes.Buffer:
		io.Copy(&buf, v)
	case []byte:
		buf.Write(v)
	case string:
		buf.WriteString(v)
	default:
		body, jsonErr := json.Marshal(data)
		if jsonErr != nil {
			return nil, jsonErr
		}
		buf.Write(body)
	}
	buf.WriteRune('\n')
	return buf.Bytes(), nil
}
