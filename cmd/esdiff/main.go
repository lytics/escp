package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

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
	src := flag.Arg(0)
	dst := flag.Arg(1)

	if !strings.HasSuffix(dst, "/") {
		dst += "/"
	}

	dice := rand.New(rand.NewSource(time.Now().UnixNano()))

	resp, err := esscroll.Start(src, timeout, pagesz, nil)
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
	var checked, matched, fastmatch, mismatch, missing uint64
	for origdoc := range resp.Hits {
		if denom == 1 || dice.Intn(denom) == 0 {
			checked++

			// Get the document from the target index
			target := fmt.Sprintf("%s/%s/%s", dst, origdoc.Type, origdoc.ID)
			resp, err := http.Get(target)
			if err != nil {
				fatalf("error contacting target: " + err.Error())
			}

			if resp.StatusCode != 200 {
				log.Printf("%d    %s", resp.StatusCode, target)
				missing++
				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				continue
			}

			newdoc := estypes.Doc{}
			if err := json.NewDecoder(resp.Body).Decode(&newdoc); err != nil {
				log.Printf("ERROR %s -- %v", target, err)
				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				continue
			}

			if origdoc.ID != newdoc.ID {
				fatalf("metadata mismatch; coding error? ID %s != %s", origdoc.ID, newdoc.ID)
			}
			if origdoc.Type != newdoc.Type {
				fatalf("metadata mismatch; coding error? Type %s != %s", origdoc.Type, newdoc.Type)
			}
			if origdoc.Index != newdoc.Index {
				fatalf("metadata mismatch; coding error? Index %s != %s", origdoc.Index, newdoc.Index)
			}

			// Fast path
			if bytes.Equal(origdoc.Source, newdoc.Source) {
				matched++
				fastmatch++
				continue
			}

			// Slow path
			origsrc := map[string]interface{}{}
			if err := json.Unmarshal(origdoc.Source, &origsrc); err != nil {
				fatalf("error unmarshalling original document; cannot compare: %s\n%v", origdoc.ID, err)
			}
			newsrc := make(map[string]interface{}, len(origsrc))
			if err := json.Unmarshal(newdoc.Source, &newsrc); err != nil {
				mismatch++
				log.Printf("DIFF  %s -- error unmarshalling target doc: %v", origdoc.ID, err)
				continue
			}

			if len(origsrc) != len(newsrc) {
				mismatch++
				log.Printf("DIFF  %s -- %d fields in source; %d fields in target", origdoc.ID, len(origsrc), len(newsrc))
				continue
			}

			if !reflect.DeepEqual(origsrc, newsrc) {
				mismatch++
				log.Printf("DIFF  %s", origdoc.ID)
				continue
			}

			// We're good!
			matched++
		}
	}

	log.Printf("Checked %d/%d (%.1f%%) documents; missing=%d mismatched=%d matched=%d (fastmatch=%d)",
		checked, resp.Total, (float64(checked)/float64(resp.Total))*100.0,
		missing, mismatch, matched, fastmatch)
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+msg+"\n", args...)
	os.Exit(2)
}
