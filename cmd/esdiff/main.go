package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/lytics/escp/esdiff"
	"github.com/lytics/escp/esscroll"
	"github.com/lytics/escp/estypes"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://host1:9200/index1/_search http://host2:9200/index2\n", os.Args[0])
		flag.PrintDefaults()
	}
	timeout := "10m"
	flag.StringVar(&timeout, "timeout", timeout, "time to keep scroll cursor alive")
	pagesz := 1000
	flag.IntVar(&pagesz, "page", pagesz, "documents to retrieve at once from each shard")
	denom := 1000
	flag.IntVar(&denom, "d", denom, "1/`N` chance of each document being checked")
	force := false
	flag.BoolVar(&force, "force", force, "continue check even if document count varies")
	flag.Parse()
	if flag.NArg() != 2 {
		fatalf("requires 2 arguments")
	}
	src := flag.Arg(0)
	dst := flag.Arg(1)

	if !strings.HasSuffix(dst, "/") {
		dst += "/"
	}

	if denom < 2 {
		denom = 1
	}
	dice := rand.New(rand.NewSource(time.Now().UnixNano()))

	resp, err := esscroll.Start(src, timeout, pagesz, 0, nil)
	if err != nil {
		fatalf("error starting scroll: %v", err)
	}

	// Make sure the totals are the same before we do a bunch of work
	{
		hresp, err := http.Get(dst + "/_search?size=0")
		if err != nil {
			fatalf("error contacting target: %v", err)
		}
		newres := estypes.Results{}
		if err := json.NewDecoder(hresp.Body).Decode(&newres); err != nil {
			fatalf("error reading target index: %v", err)
		}
		if resp.Total != newres.Hits.Total {
			log.Printf("Source and target have different document totals: %d vs. %d", resp.Total, newres.Hits.Total)
			if !force {
				return
			}
		}
	}

	log.Printf("Scrolling over %d documents\n", resp.Total)
	var checked, matched, mismatch, missing uint64
	for doc := range resp.Hits {
		if denom == 1 || dice.Intn(denom) == 0 {
			checked++
			diff, err := esdiff.Check(doc, dst)
			if err != nil {
				fatalf("fatal error: %v", err)
			}
			switch diff {
			case "":
				matched++
			case esdiff.DiffMissing:
				missing++
				fmt.Println("MISS ", doc.ID)
			default:
				mismatch++
				fmt.Println("DIFF ", doc.ID)
			}
		}
	}

	log.Printf("Checked %d/%d (%.1f%%) documents; missing=%d mismatched=%d matched=%d",
		checked, resp.Total, (float64(checked)/float64(resp.Total))*100.0,
		missing, mismatch, matched)

	if missing+mismatch > 0 {
		os.Exit(99)
	}
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+msg+"\n", args...)
	flag.Usage()
	os.Exit(2)
}
