package esbulk

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/lytics/escp/estypes"
)

// ErrClosed is returned when a method is called on a closed indexer. Callers
// receiving this error should check the Indexer.Err() method to see if the
// bulk indexer terminated due to error.
var ErrClosed = errors.New("already closed")

type Indexer struct {
	count uint64
	docs  <-chan *estypes.Doc
	err   chan error
}

func (i *Indexer) Err() chan error { return i.err }

// NewIndexer creates a new Elasticsearch bulk indexer. URL should be of the
// form http://eshost:9200/_bulk.
//
// Sends to docs should select on Indexer.Err to prevent deadlocking in case of
// indexer error.
func NewIndexer(url string, docs <-chan *estypes.Doc) *Indexer {
	indexer := &Indexer{
		docs: docs,

		// buffer of 1 to allow goroutine to exit before value is read
		err: make(chan error, 1),
	}

	go func() {
		// Batches are flushed when they exceed 25mb, so give the buffer plenty of
		// breathing room to avoid resizing.
		const uploadat = 25 * 1024 * 1024
		const bufsz = 40 * 1024 * 1024
		buf := bytes.NewBuffer(make([]byte, 0, bufsz))
		for doc := range docs {
			enc := json.NewEncoder(buf)
			action := estypes.BulkAction{Index: &doc.Meta}
			if err := enc.Encode(&action); err != nil {
				indexer.err <- err
				return
			}
			if err := enc.Encode(&doc.Source); err != nil {
				indexer.err <- err
				return
			}
			if buf.Len() >= uploadat {
				//TODO upload and reset buffer
			}
		}
	}()

	return indexer
}
