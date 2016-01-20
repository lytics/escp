package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/lytics/escp/esbulk"
	"github.com/lytics/escp/esindex"
	"github.com/lytics/escp/esscroll"
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

	// Copy over shards setting if it wasn't explicitly set
	if shards == 0 {
		idxmeta, err := esindex.Get(src)
		if err != nil {
			fatalf("failed getting source index settings: %v", err)
		}
		shards = idxmeta.Settings.Index.Shards
	}

	// Create the destination index unless explicitly told not to
	if !skipcreate {
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{Shards: shards}}}
		if err := esindex.Create(dst, &m); err != nil {
			fatalf("failed creating index: %v", err)
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
