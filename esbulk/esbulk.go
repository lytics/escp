package esbulk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/lytics/escp/esscroll"
	"github.com/lytics/escp/estypes"
	log "github.com/lytics/escp/logging"
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

// New creates a new Elasticsearch bulk indexer. URL should be of the
// form http://eshost:9200/_bulk.
//
// bufsz is the size of the upload buffer in kilobytes. bufsz < 1 will default
// to 20mb.
//
// par is the number of parallel buffers to use. par < 1 will default to 3.
//
// Sends to docs should select on Indexer.Err to prevent deadlocking in case of
// indexer error.
func New(ctx context.Context, hosts []string, index string, bufsz, par int, docs <-chan *estypes.Doc, logger log.Logger) *Indexer {
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
		targets[i] = fmt.Sprintf("%s/_bulk", h)
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
		batchs := make(chan *Batch, par)
		for i := 0; i < par; i++ {
			batchs <- NewBatch()
		}

		var batch *Batch = nil
		var sz = 0
		for doc := range docs {
			if batch == nil {
				b := <-batchs
				b.Reset()
				batch = b
			}

			batch.Add(doc.ID, doc)
			sz += len(doc.Source)

			// Actually do the bulk insert once the buffer is full
			if sz >= uploadat {
				wg.Add(1)
				go func(b *Batch, target string) {
					defer wg.Done()
					if b.Len() == 0 {
						return
					}
					if err := upload(target, index, b, logger); err != nil {
						indexer.err <- err
						return
					}
					batchs <- b
				}(batch, targets[ti])

				sz = 0
				batch = nil                  // go to next buffer in buffer pool
				ti = (ti + 1) % len(targets) // go to the next host
			}
			select {
			case <-ctx.Done():
				//TODO Save prgress
				return
			default:
			}
		}

		// No more docs, if the buffer is non-empty upload it
		if batch != nil && batch.Len() > 0 {
			ti = (ti + 1) % len(targets)
			if err := upload(targets[ti], index, batch, logger); err != nil {
				indexer.err <- err
			}
		}
		wg.Wait() // wait for async uploads to complete too
	}()

	return indexer
}

// upload buffer to bulk API.
func upload(url, index string, batch *Batch, logger log.Logger) error {
	for try := 0; try < 16; try++ {
		buf, err := batch.Encode(index)
		if err != nil {
			return fmt.Errorf("esbulk.upload: error encoding batch: %v", err)
		}
		if len(buf) == 0 {
			logger.Infof("length of buffer to write is 0, skipping")
			time.Sleep(1 * time.Second)
			continue
		}

		if try > 10 {
			logger.Warnf("slow upload warning: retry:%v bytes:%v batchlen:%v", try, esscroll.IECFormat(uint64(len(buf))), batch.Len())
		}

		resp, err := Client.Post(url, "application/json", bytes.NewReader(buf))
		if err != nil {
			logger.Warnf("esbulk.upload: error posting to ES: %v, bytes len: %d", err, len(buf))
			backoff(try)
			continue
		}
		if resp != nil {
			defer resp.Body.Close()
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("esbulk.upload: error reading response: %v", err)
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("esbulk.upload: non-200 response code: %d", resp.StatusCode)
		}
		bresp := &BulkResponses{}
		if err := json.Unmarshal(b, &bresp); err != nil {
			return fmt.Errorf("esbulk.upload: error decoding response: %v", err)
		}
		//log.Printf("BulkResponse successes: %d\n", len(bresp.Items))

		ct := 0
		const include404 = false
		for _, successful := range bresp.Succeeded(include404) {
			// remove bulk successes from next try, so we only resent the
			// failed docs.
			batch.Delete(successful.Id)
			ct++
		}
		if batch.Len() == 0 {
			break
		}

		backoff(try)
	}

	if batch.Len() > 0 {
		logger.Errorf("error: unable to write all docs to ES for this batch: %v remaining items", batch.Len())
	}
	batch.Reset()

	return nil
}

func backoff(try int) {
	nf := math.Pow(2, float64(try))
	nf = math.Max(1, nf)
	nf = math.Min(nf, 1024)
	r := rand.Int31n(int32(nf))
	d := time.Duration(int32(try*100)+r) * time.Millisecond
	time.Sleep(d)
}
