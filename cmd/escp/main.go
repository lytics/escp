package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/lytics/escp/esbulk"
	"github.com/lytics/escp/esscroll"
	"github.com/lytics/escp/estypes"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://HOST1:9200/INDEX1 http://HOST2:9200/INDEX2\n", os.Args[0])
		flag.PrintDefaults()
	}
	shards := 0
	flag.IntVar(&shards, "shards", shards, "number of shards target index will have (default = same as old index)")
	skipcreate := false
	flag.BoolVar(&skipcreate, "skipcreate", skipcreate, "skip destination index creation")
	flag.Parse()
	if flag.NArg() != 2 {
		fatalf("expected 2 arguments, found %d\n", flag.NArg())
	}
	if shards > 0 && skipcreate {
		fatalf("cannot set shards and skip index creation")
	}

	src := flag.Arg(0)
	if strings.HasSuffix(src, "/") {
		src = src[:len(src)-1]
	}
	dst := flag.Arg(1)
	if strings.HasSuffix(dst, "/") {
		dst = dst[:len(dst)-1]
	}
	dstparts := strings.Split(dst, "/")
	idx := dstparts[len(dstparts)-1]

	// Start the scroll first to make sure the source parameter is valid
	resp, err := esscroll.Start(src+"/_search", "10m", 1000, nil)
	if err != nil {
		fatalf("error starting scroll: %v", err)
	}

	// Next create the index
	if shards == 0 {
		// Get shards from original index
		resp, err := http.Get(src)
		if err != nil {
			fatalf("error contacting source Elasticsearch: %v", err)
		}
		if resp.StatusCode != 200 {
			fatalf("non-200 status code from source Elasticsearch: %d", resp.StatusCode)
		}

		idxmetamap := make(map[string]*estypes.IndexMeta, 1)
		if err := json.NewDecoder(resp.Body).Decode(&idxmetamap); err != nil {
			fatalf("error decoding index metadata: %v", err)
		}
		parts := strings.Split(src, "/")
		idxname := parts[len(parts)-1]
		idxmeta, ok := idxmetamap[idxname]
		if !ok {
			fatalf("index %s not found", idxname)
		}
		if idxmeta.Settings.Index.Shards == 0 {
			fatalf("unable to read existing shards for index %s", idxname)
		}
		shards = idxmeta.Settings.Index.Shards
	}

	if !skipcreate {
		buf, err := json.Marshal(&estypes.IndexMeta{Settings: &estypes.Settings{Index: &estypes.IndexSettings{Shards: shards}}})
		if err != nil {
			fatalf("error encoding index creation json: %v", err)
		}
		req, err := http.NewRequest("PUT", dst, bytes.NewReader(buf))
		if err != nil {
			fatalf("error creating index creation request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fatalf("error creating index %s: %v", dst, err)
		}
		ackr := estypes.AckResponse{}
		if err := json.NewDecoder(resp.Body).Decode(&ackr); err != nil {
			fatalf("error decoding index creation response: %v", err)
		}
		if !ackr.Ack {
			fatalf("failed to create destination index %s", dst)
		}
	}

	log.Printf("Copying %d documents from %s to %s", resp.Total, src, dst)

	indexer := esbulk.NewIndexer(dst+"/_bulk", idx, resp.Hits)

	if err := <-indexer.Err(); err != nil {
		log.Fatalf("Error indexing: %v", err)
	}

	if err := resp.Err(); err != nil {
		log.Fatalf("Error searching: %v", err)
	}

	log.Printf("Completed")
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+msg+"\n", args...)
	flag.Usage()
	os.Exit(2)
}
