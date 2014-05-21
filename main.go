package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

var magnitudes = map[byte]int{
	'b': 1,
	'k': 1024,
	'm': 1024 * 1024,
	'g': 1024 * 1024 * 1024,
	// Going above gigabytes for ints doesn't make sense
}

func toBytes(v string) (int, error) {
	if len(v) < 2 {
		return 0, fmt.Errorf("size string too small: %s", v)
	}
	n, err := strconv.Atoi(v[:len(v)-1])
	if err != nil {
		return 0, err
	}
	mag, ok := magnitudes[v[len(v)-1]]
	if !ok {
		return 0, fmt.Errorf("invalid order of magnitude: %s", v)
	}
	return n * mag, nil
}

func toHumans(b int) string {
	for _, k := range []byte("gmk") {
		m := magnitudes[k]
		if b > m {
			return fmt.Sprintf("%.2v%c", float64(b)/float64(m), k)
		}
	}
	return fmt.Sprintf("%db", b)
}

// ElasticSearch's default max upload size is 100 MB, we need to be plenty below that
const esMaxUploadSize = 15 * 1024 * 1024

type Meta struct {
	ID    string `json:"_id"`
	Type  string `json:"_type"`
	Index string `json:"_index"`
}

type Doc struct {
	Meta
	Source json.RawMessage `json:"_source,omitempty"`
}

type Results struct {
	Hits *struct {
		Hits []*Doc `json:"hits"`
	} `json:"hits"`
	TimedOut bool   `json:"timed_out"`
	ScrollID string `json:"_scroll_id"`
}

func uploader(wg *sync.WaitGroup, target string, docChan <-chan []*Doc, size int) {
	defer wg.Done()
	targetURL, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	bulkURL := targetURL.Scheme + "://" + targetURL.Host + "/_bulk"
	targetIndex := targetURL.Path[1:] // Strip initial /

	envelope := &struct {
		Index Meta `json:"index"`
	}{}
	buf := bytes.NewBuffer(make([]byte, 0, size))
	enc := json.NewEncoder(buf)

	for docs := range docChan {
		for _, doc := range docs {
			envelope.Index = doc.Meta
			envelope.Index.Index = targetIndex
			if err := enc.Encode(envelope); err != nil {
				panic(err.Error())
			}
			// Strip newlines in the source
			for _, c := range doc.Source {
				if c != '\n' {
					buf.WriteByte(c)
				}
			}
			buf.WriteByte('\n')
			if buf.Len() >= size {
				doUpload(bulkURL, buf.Bytes())
				buf = bytes.NewBuffer(make([]byte, 0, size))
				enc = json.NewEncoder(buf)
			}
		}
	}
	if buf.Len() > 0 {
		doUpload(bulkURL, buf.Bytes())
	}
}

func doUpload(target string, body []byte) {
	start := time.Now()
	sz := toHumans(len(body))
	resp, err := http.DefaultClient.Post(target, "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Printf("Error uploading to %s: %v\n", target, err)
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("Non-200 status code on upload: %d %s\n", resp.StatusCode, target)
		raw, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(raw))
		fmt.Println()
		fmt.Println(string(body))
	} else {
		fmt.Printf("Uploaded %s of documents in %s\n", sz, time.Now().Sub(start))
	}
}

func main() {
	eta := flag.String("eta", "10m", "time to keep scroll cursor alive")
	batch := flag.Int("batch", 1000, "documents to retrieve at once from each shard")
	size := flag.String("size", toHumans(esMaxUploadSize), "size of each bulk upload")
	scrollFlag := flag.String("scroll", "", "scroll ID to continue from")
	filterFlag := flag.String("filter", "", "optional filter to apply")
	flag.Parse()
	if flag.NArg() != 2 {
		UsageExitErr(fmt.Sprintf("Expected 2 arguments, found %d\n", flag.NArg()))
	}

	src := flag.Arg(0)
	dst := flag.Arg(1)

	srcURL, err := url.Parse(src)
	if err != nil {
		UsageExitErr(fmt.Sprintf("Bad source URL %s: %v", src, err))
	}

	rawSize, err := toBytes(*size)
	if err != nil {
		UsageExitErr(fmt.Sprintf("Invalid upload size %s: %v", *size, err))
	}

	if *filterFlag != "" && *scrollFlag != "" {
		UsageExitErr("Cannot set `filter` and `scroll`.")
	}

	var filter map[string]interface{}
	if *filterFlag != "" {
		if err := json.Unmarshal([]byte(*filterFlag), &filter); err != nil {
			UsageExitErr(fmt.Sprintf("`filter` is not valid JSON: %v", err))
		}
		if _, ok := filter["filter"]; ok {
			UsageExitErr("`filter` should not include `filter` key")
		}
	}

	uploadChan := make(chan []*Doc, 2) // allow a batches to buffer a bit
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go uploader(wg, dst, uploadChan, int(rawSize))

	s := time.Now()
	done := download(srcURL, *scrollFlag, *eta, *batch, filter, uploadChan)
	fmt.Printf("Finished downloading %d documents in %s.\n", done, time.Now().Sub(s))
	close(uploadChan)
	wg.Wait()
}

// download pulls documents from src and sends them to uploadChan.
func download(src *url.URL, scrollID, eta string, batch int, filter map[string]interface{}, uploadChan chan []*Doc) uint64 {
	if scrollID == "" {
		srcVals := url.Values{
			"scroll":      []string{eta},
			"search_type": []string{"scan"},
			"size":        []string{strconv.Itoa(batch)},
		}.Encode()
		searchURL := fmt.Sprintf("%s/_search?%s", src, srcVals)

		// Start search
		var err error
		var resp *http.Response
		if filter == nil {
			resp, err = http.DefaultClient.Get(searchURL)
		} else {
			req := struct {
				Filter map[string]interface{} `json:"filter"`
			}{filter}
			body, err := json.Marshal(req)
			if err != nil {
				// We were able to decode the filter, so being unable to encode it
				// should never happen.
				panic(err)
			}
			resp, err = http.DefaultClient.Post(searchURL, "application/json", bytes.NewReader(body))
		}
		if err != nil {
			fmt.Printf("Error connecting to %s: %v", searchURL, err)
			os.Exit(1)
		}
		if resp.StatusCode != 200 {
			fmt.Printf("Non-200 status code on search: %d %s\n", resp.StatusCode, searchURL)
			os.Exit(2)
		}
		scrollResp := &struct {
			Hits *struct {
				Total int64 `json:"total"`
			} `json:"hits"`
			ScrollID string `json:"_scroll_id"`
		}{}
		if err := json.NewDecoder(resp.Body).Decode(scrollResp); err != nil {
			panic(err)
		}

		scrollID = scrollResp.ScrollID
		fmt.Printf("Started scrolling over %d documents with ID %s", scrollResp.Hits.Total, scrollID)
	}
	baseSrcURL := src.Scheme + "://" + src.Host + "/" + "_search/scroll"
	deleteURL := fmt.Sprintf("%s?%s", baseSrcURL, url.Values{"scroll_id": []string{scrollID}}.Encode())
	defer func() {
		req, err := http.NewRequest("DELETE", deleteURL, nil)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode != 200 {
			log.Fatalf("Non-200 status code when deleting cursor: %d", resp.StatusCode)
		}
	}()

	scrollVals := &url.Values{"scroll_id": []string{scrollID}, "scroll": []string{eta}}
	var done uint64 = 0
	for i := 0; ; i++ {
		innerStart := time.Now()
		resp, err := http.DefaultClient.Get(baseSrcURL + "?" + scrollVals.Encode())
		if err != nil {
			panic(err)
		}
		result := new(Results)
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			panic(err)
		}
		if resp.StatusCode != 200 {
			fmt.Printf("Non-200 status code on scroll %d: %d %s\n", i, resp.StatusCode, scrollVals.Encode())
			os.Exit(2)
		}
		scrollVals.Set("scroll_id", result.ScrollID)
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Error closing scroll %d response: %v\n", i, err)
		}
		if len(result.Hits.Hits) == 0 {
			fmt.Printf("Done scrolling over %d documents in %d requests.\n", done, i)
			break
		}

		uploadChan <- result.Hits.Hits
		numDocs := len(result.Hits.Hits)
		done += uint64(numDocs)
		fmt.Printf("Retrieved %d documents in %s. Scroll ID: %s \n", numDocs, time.Now().Sub(innerStart), result.ScrollID)
	}
	return done
}

func UsageExitErr(msg string) {
	if msg != "" {
		fmt.Fprintf(os.Stderr, "\n%s\n", msg)
	}
	fmt.Fprintf(os.Stderr, `Usage: 

%s [options] \
		http://localhost:9200/source_index \
		http://locahost:9200/destination_index
	  %s`, os.Args[0], "\n\n")
	flag.PrintDefaults()
	os.Exit(1)
}
