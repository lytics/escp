package esbulk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

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
// Sends to docs should select on Indexer.Err to prevent deadlocking in case of
// indexer error.
func NewIndexer(url string, index string, docs <-chan *estypes.Doc) *Indexer {
	indexer := &Indexer{
		docs: docs,

		// buffer of 1 to allow goroutine to exit before value is read
		err: make(chan error, 1),
	}

	go func() {
		defer close(indexer.err)

		// Batches are flushed when they exceed 25mb, so give the buffer plenty of
		// breathing room to avoid resizing.
		const uploadat = 25 * 1024 * 1024
		const bufsz = 40 * 1024 * 1024
		buf := bytes.NewBuffer(make([]byte, 0, bufsz))
		action := estypes.BulkAction{}
		enc := json.NewEncoder(buf)
		for doc := range docs {

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
				if err := upload(url, buf.Bytes()); err != nil {
					indexer.err <- err
					return
				}

				// Reset the buffer
				buf.Reset()
			}
		}

		// No more docs, if the buffer is non-empty upload it
		if buf.Len() > 0 {
			if err := upload(url, buf.Bytes()); err != nil {
				indexer.err <- err
			}
		}
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
	bresp := estypes.BulkResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&bresp); err != nil {
		return fmt.Errorf("esbulk: error decoding response: %v", err)
	}
	if bresp.Errors {
		return fmt.Errorf("esbulk: errors encountered when indexing")
	}
	return nil
}
