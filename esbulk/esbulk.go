package esbulk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/lytics/escp/estypes"
)

var Client = http.DefaultClient

// ErrClosed is returned when a method is called on a closed indexer. Callers
// receiving this error should check the Indexer.Err() method to see if the
// bulk indexer terminated due to error.
var ErrClosed = errors.New("already closed")

type Indexer struct {
	count uint64
	docs  <-chan *estypes.Doc
	err   chan error
}

// Err allows monitoring for errors while indexing is occurring. It will be
// closed when indexing is finished.
func (i *Indexer) Err() chan error { return i.err }

// NewIndexer creates a new Elasticsearch bulk indexer. URL should be of the
// form http://eshost:9200/_bulk.
//
// bufsz is the size of the upload buffer in kilobytes. bufsz < 1 will default
// to 20mb.
//
// par is the number of parallel buffers to use. par < 1 will default to 3.
//
// Sends to docs should select on Indexer.Err to prevent deadlocking in case of
// indexer error.
func NewIndexer(hosts []string, index string, bufsz, par int, docs <-chan *estypes.Doc) *Indexer {
	indexer := &Indexer{
		docs: docs,

		// buffer an error per parallel upload buffer
		err: make(chan error, par),
	}

	if bufsz < 1 {
		bufsz = 20 * 1024
	}
	if par < 1 {
		par = 3
	}
	targets := make([]string, len(hosts))
	for i, h := range hosts {
		targets[i] = fmt.Sprintf("http://%s/_bulk", h)
	}
	ti := 0

	go func() {
		defer close(indexer.err)

		uploadat := bufsz
		if bufsz > 1000 {
			// upload at 500kb less than buffer size to avoid buffer resizing
			uploadat = bufsz - 500
		}

		wg := new(sync.WaitGroup)
		bufs := make(chan *bytes.Buffer, par)
		for i := 0; i < par; i++ {
			bufs <- bytes.NewBuffer(make([]byte, 0, bufsz))
		}
		action := BulkAction{}
		var enc *json.Encoder
		var buf *bytes.Buffer

		for doc := range docs {
			if buf == nil {
				buf = <-bufs
				enc = json.NewEncoder(buf)
			}

			// Write action
			action.Index = &doc.Meta
			action.Index.Index = index
			if err := enc.Encode(&action); err != nil {
				indexer.err <- err
				return
			}

			// Write document
			if err := enc.Encode(&doc.Source); err != nil {
				indexer.err <- err
				return
			}

			// Actually do the bulk insert once the buffer is full
			if buf.Len() >= uploadat {
				wg.Add(1)
				go func(b *bytes.Buffer) {
					defer wg.Done()
					if err := upload(targets[ti], b.Bytes()); err != nil {
						indexer.err <- err
						return
					}

					// Reset the buffer
					b.Reset()
					bufs <- b
				}(buf)

				buf = nil                    // go to next buffer in buffer pool
				ti = (ti + 1) % len(targets) // go to the next host
			}
		}

		// No more docs, if the buffer is non-empty upload it
		if buf != nil && buf.Len() > 0 {
			ti = (ti + 1) % len(targets)
			if err := upload(targets[ti], buf.Bytes()); err != nil {
				indexer.err <- err
			}
		}
		wg.Wait() // wait for async uploads to complete too
	}()

	return indexer
}

// upload buffer to bulk API.
func upload(url string, buf []byte) error {
	resp, err := Client.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		ioutil.ReadAll(resp.Body)
		return fmt.Errorf("esbulk: non-200 response code: %d", resp.StatusCode)
	}
	bresp := BulkResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&bresp); err != nil {
		return fmt.Errorf("esbulk: error decoding response: %v", err)
	}
	if bresp.Errors {
		return fmt.Errorf("esbulk: errors encountered when indexing")
	}
	return nil
}
