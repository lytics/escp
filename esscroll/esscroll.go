package esscroll

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/lytics/escp/estypes"
	log "github.com/lytics/escp/logging"
)

var Client = http.DefaultClient

type Response struct {
	Total uint64
	Hits  <-chan *estypes.Doc

	mu  *sync.Mutex
	err error
}

func (r *Response) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *Response) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

type ESScoll struct {
	surl    string
	timeout string
	pagesz  int
	buflen  int
	filter  map[string]interface{}

	logevery time.Duration
	logger   log.Logger
	ctx      context.Context
}

func New(ctx context.Context, indexUrl string, timeout time.Duration, pagesz, buflen int, filter map[string]interface{}, logevery time.Duration, logger log.Logger) *ESScoll {
	surl := indexUrl + "/_search"
	tout := fmt.Sprintf("%ds", int(timeout.Seconds()))
	return &ESScoll{
		surl:     surl,
		timeout:  tout,
		pagesz:   pagesz,
		buflen:   buflen,
		filter:   filter,
		logevery: logevery,
		logger:   logger,
		ctx:      ctx,
	}
}

// Start a new scroll. URL should be of the form http://host:port/indexname.
//
// When Response.Hits is closed, Response.Err() should be checked to see if the
// scroll completed successfully or not.
func (s *ESScoll) Start() (*Response, error) {
	origurl, err := url.Parse(s.surl)
	if err != nil {
		return nil, err
	}
	searchurl := fmt.Sprintf("%s?scroll=%s&size=%d", s.surl, s.timeout, s.pagesz)

	var resp *http.Response
	if s.filter == nil {
		resp, err = http.DefaultClient.Get(searchurl)
	} else {
		req := struct {
			Filter map[string]interface{} `json:"filter"`
		}{s.filter}
		body, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		resp, err = http.DefaultClient.Post(searchurl, "application/json", bytes.NewReader(body))
	}

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non-200 status code on intial request %d from %v", resp.StatusCode, searchurl)
	}

	result := estypes.Results{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.TimedOut {
		return nil, fmt.Errorf("initial scroll timed out")
	}
	if result.Hits == nil {
		return nil, fmt.Errorf("invalid response")
	}

	out := make(chan *estypes.Doc, s.buflen) // each result will actually get pagesz*shards documents
	r := Response{Total: result.Hits.Total, Hits: out, mu: new(sync.Mutex)}

	go func() {
		defer close(out)
		ctx, can := context.WithCancel(s.ctx)
		prog := NewProgress(s.logevery, s.logger)
		prog.Start(ctx)
		prog.SetDocCount(r.Total)

		docspages := make(chan []*estypes.Doc, 2)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func(wg *sync.WaitGroup, docspages chan []*estypes.Doc) {
			defer wg.Done()
			for hits := range docspages {
				select {
				case <-s.ctx.Done():
					//TODO Save prgress
					return
				default:
				}
				for _, hit := range hits {
					st := time.Now()
					out <- hit
					prog.MarkBlocked(time.Now().Sub(st))
				}
			}
		}(wg, docspages)

		baseurl := origurl.Scheme + "://" + origurl.Host + "/_search/scroll?scroll=" + s.timeout + "&scroll_id="

		//TODO the array of docs all the way into esbulk
		//TODO copy ScrollID with page
		cnt := 0
		for {
			select {
			case <-s.ctx.Done():
				//TODO Save prgress
				return
			default:
			}
			// Get the next page
			urli := baseurl + result.ScrollID
			//s.logger.Infof("fetching from:%v", urli)
			resp, err = Client.Get(urli)
			if err != nil {
				r.setErr(err)
				return
			}
			if resp.StatusCode != 200 {
				resp.Body.Close()
				r.setErr(fmt.Errorf("non-200 status code on continuation %d", resp.StatusCode))
				return
			}

			// Reset and decode results
			result = estypes.Results{}
			if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
				resp.Body.Close()
				r.setErr(err)
				return
			}
			if result.TimedOut {
				r.setErr(fmt.Errorf("timed-out on scroll"))
				return
			}

			if len(result.Hits.Hits) == 0 {
				//defer s.logger.Infof("completed: %v ", cnt)
				can()
				close(docspages)
				wg.Wait()
				return
			}
			cnt++
			hits := result.Hits.Hits
			docspages <- hits
			prog.MarkProssed(len(result.Hits.Hits))
		}
	}()

	return &r, nil
}

//TODO move this progress to it's own package and share it with esbulk so we collect retry and error, and other metrics.
func NewProgress(logevery time.Duration, logger log.Logger) *progress {
	return &progress{
		logevery: logevery,
		logger:   logger,
	}
}

type progress struct {
	logevery time.Duration
	logger   log.Logger

	mu             sync.Mutex
	last           time.Time
	processed      uint64
	totalprocessed uint64
	blockedtotal   time.Duration
	blockedcnt     int
	expectedDocs   uint64
	starttime      time.Time
}

func (p *progress) SetDocCount(n uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expectedDocs = uint64(n)
}

func (p *progress) MarkBlocked(blockedDur time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blockedcnt += 1
	p.blockedtotal += blockedDur
}

func (p *progress) MarkProssed(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	n2 := uint64(n)
	p.totalprocessed += n2
	p.processed += n2
}
func (p *progress) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.starttime = time.Now()
	p.last = time.Now()

	go func() {
		frsLog := time.After(20 * time.Second)
		for {
			select {
			case <-frsLog:
				p.log()
			case <-time.After(p.logevery):
				p.log()
			case <-ctx.Done():
				p.log()
				return
			}
		}
	}()
}
func (p *progress) log() {
	p.mu.Lock()
	defer p.mu.Unlock()

	elapsed := time.Now().Sub(p.last)
	totalelapsed := time.Now().Sub(p.starttime)
	avetimeWaintToSend := time.Duration(int64(p.blockedtotal) / int64(max(1, p.blockedcnt)))
	processsedSec := p.processed / uint64(math.Max(1, elapsed.Seconds()))
	totalProcesssedSec := p.totalprocessed / uint64(math.Max(1, totalelapsed.Seconds()))

	p.logger.Infof("%d / %d documents scrolled (doc_rate:[total:%d docs/s curr:%d docs/s]) (average chan send time:%v)",
		p.totalprocessed, p.expectedDocs, totalProcesssedSec, processsedSec, avetimeWaintToSend)

	p.last = time.Now()
	p.processed = 0
}

//IECFormat prints bytes in the International Electro-technical Commission format
//http://play.golang.org/p/68w_QCsE4F
// multiples of 1024
func IECFormat(num_in uint64) string {
	suffix := "B" //just assume bytes
	num := float64(num_in)
	units := []string{"", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"}
	for _, unit := range units {
		if num < 1024.0 {
			return fmt.Sprintf("%3.1f%s%s", num, unit, suffix)
		}
		num = (num / 1024)
	}
	return fmt.Sprintf("%.1f%s%s", num, "Yi", suffix)
}

//TODO Implement continuing an already started scroll
//func Continue(url, scrollID string) {}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
