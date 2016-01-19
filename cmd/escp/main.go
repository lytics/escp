package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/lytics/escp/esbulk"
	"github.com/lytics/escp/esscroll"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://HOST1:9200/INDEX2/_search http://HOST2:9200/_bulk INDEX2\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 3 {
		fatalf("Expected 3 arguments, found %d\n", flag.NArg())
	}

	src := flag.Arg(0)
	dst := flag.Arg(1)
	idx := flag.Arg(2)

	resp, err := esscroll.Start(src, "10m", 1000, nil)
	if err != nil {
		fatalf("error starting scroll: %v", err)
	}

	log.Printf("Copying %d documents from %s to %s index %s ", resp.Total, src, dst, idx)

	indexer := esbulk.NewIndexer(dst, idx, resp.Hits)

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
