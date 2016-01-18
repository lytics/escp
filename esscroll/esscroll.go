package esscroll

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/lytics/escp/estypes"
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

// Start a new scroll. URL should be of the form http://host:port/indexname.
//
// When Response.Hits is closed, Response.Err() should be checked to see if the
// scroll completed successfully or not.
func Start(surl, timeout string, pagesz int, filter map[string]interface{}) (*Response, error) {
	origurl, err := url.Parse(surl)
	if err != nil {
		return nil, err
	}
	searchurl := fmt.Sprintf("%s?search_type=scan&scroll=%s&size=%d", surl, timeout, pagesz)

	var resp *http.Response
	if filter == nil {
		resp, err = http.DefaultClient.Get(searchurl)
	} else {
		req := struct {
			Filter map[string]interface{} `json:"filter"`
		}{filter}
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
		return nil, fmt.Errorf("non-200 status code on intial request %d", resp.StatusCode)
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

	out := make(chan *estypes.Doc, pagesz) // each result will actually get pagesz*shards documents
	r := Response{Total: result.Hits.Total, Hits: out, mu: new(sync.Mutex)}

	go func() {
		defer close(out)

		baseurl := origurl.Scheme + "://" + origurl.Host + "/_search/scroll?scroll=" + timeout + "&scroll_id="

		for {
			// Get the next page
			resp, err = Client.Get(baseurl + result.ScrollID)
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
				// Completed!
				return
			}

			for _, hit := range result.Hits.Hits {
				out <- hit
			}
		}
	}()

	return &r, nil
}

//TODO Implement continuing an already started scroll
//func Continue(url, scrollID string) {}
