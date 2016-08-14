package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lytics/escp/esbulk"
	"github.com/lytics/escp/esindex"
	"github.com/lytics/escp/esscroll"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://SRCHOST1:9200/INDEX1 DESHOST2:9200,DESHOST3:9200,DESHOST4:9200 INDEX2\n", os.Args[0])
		flag.PrintDefaults()
	}

	// Index creation settings
	shards := 0
	flag.IntVar(&shards, "shards", shards, "number of shards target index will have (default = same as old index)")
	skipcreate := false
	flag.BoolVar(&skipcreate, "skipcreate", skipcreate, "skip destination index creation")

	// Tunables
	scrolltimeout := "15m"
	flag.StringVar(&scrolltimeout, "scrolltime", scrolltimeout, "time to keep scroll alive between requests")
	scrollpage := 1000
	flag.IntVar(&scrollpage, "scrollpage", scrollpage, "size of scroll pages (will actually be per source shard)")
	scrolldocs := 5000
	flag.IntVar(&scrolldocs, "scrolldocs", scrolldocs, "number of `docs` to buffer in memory from scroll")
	bulksz := 128 * 1024
	flag.IntVar(&bulksz, "bulksz", bulksz, "size of bulk upload buffer in `KB`")
	bulkpar := 0
	flag.IntVar(&bulkpar, "bulkpar", bulkpar, "number of parallel bulk upload buffers to use; 0 = len(hosts)*2")

	delayrefresh := true
	flag.BoolVar(&delayrefresh, "delayrefresh", delayrefresh, "delay refresh until bulk indexing is complete")
	delayreplicaton := false
	flag.BoolVar(&delayreplicaton, "delayreplicaton", delayreplicaton, "delay replicaiton until bulk indexing is complete.  requires --replicationfactor=n")
	replicationfactor := 1
	flag.IntVar(&replicationfactor, "replicationfactor", replicationfactor, "if delayreplication is set the replicaiton setting will be set to this after coping.")

	refreshint := ""
	flag.StringVar(&refreshint, "refreshint", refreshint, "if indexing is delayed, what to set the refresh interval to after copy; defaults to old index's setting or 1s")
	maxsegs := 5
	flag.IntVar(&maxsegs, "maxsegs", maxsegs, "if indexing is delayed, the max number of segments for the optimized index")
	createdelay := time.Second
	flag.DurationVar(&createdelay, "createdelay", createdelay, "time to sleep after index creation to let cluster go green")

	flag.Parse()
	if flag.NArg() != 3 {
		fatalf("expected 3 arguments, found %d\n", flag.NArg())
	}
	if shards > 0 && skipcreate {
		fatalf("cannot set shards and skip index creation")
	}

	src := flag.Arg(0)
	if strings.HasSuffix(src, "/") {
		src = src[:len(src)-1]
	}
	dsts := strings.Split(flag.Arg(1), ",")
	if len(dsts) < 1 {
		fatalf("need at least one destination host")
	}
	if bulkpar == 0 {
		bulkpar = len(dsts) * 2
	}
	idx := flag.Arg(2)

	// Use the first destination host as the "primary"
	pridst := fmt.Sprintf("http://%s/%s", dsts[0], idx)

	idxmeta, err := esindex.Get(src)
	if err != nil {
		fatalf("failed getting source index metadata: %v", err)
	}

	// Copy over shards setting if it wasn't explicitly set
	if shards == 0 {
		shards = *idxmeta.Settings.Index.Shards
	}

	// Copy over refreshint if it wasn't set in options but was set on the source
	// index
	if refreshint == "" {
		if idxmeta.Settings.Index.RefreshInterval != "" {
			refreshint = idxmeta.Settings.Index.RefreshInterval
		} else {
			refreshint = "1s" // default
		}
	}

	// Start the scroll first to make sure the source parameter is valid
	resp, err := esscroll.Start(src+"/_search", scrolltimeout, scrollpage, scrolldocs, nil)
	if err != nil {
		fatalf("error starting scroll: %v", err)
	}

	// Create the destination index unless explicitly told not to
	if !skipcreate {
		log.Printf("Creating index %s with shards=%d refresh_interval=%s delay-refresh=%t", idx, shards, refreshint, delayrefresh)
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{
			Shards:          &shards,
			RefreshInterval: refreshint,
		}}}
		if delayrefresh {
			// Disable refreshing until end
			m.Settings.Index.RefreshInterval = "-1"
		}
		if delayreplicaton {
			i := 0
			m.Settings.Index.Replicas = &i
		}
		if err := esindex.Create(pridst, &m); err != nil {
			fatalf("failed creating index: %v", err)
		}

		time.Sleep(createdelay)
	}

	desmeta, err := esindex.Get(pridst)
	if err != nil {
		log.Printf("error loading destination index settings. err:%v", err)
	}
	b, err := json.Marshal(desmeta)
	if err != nil {
		log.Printf("error marshalling index settings. err:%v", err)
	}

	log.Printf("Copying %d documents from %s to %s/%s destination index settings: %v", resp.Total, src, flag.Arg(1), idx, string(b))

	indexer := esbulk.NewIndexer(dsts, idx, bulksz, bulkpar, resp.Hits)

	if err := <-indexer.Err(); err != nil {
		log.Fatalf("Error indexing: %v", err)
	}

	if err := resp.Err(); err != nil {
		log.Fatalf("Error searching: %v", err)
	}

	if delayrefresh {
		log.Printf("Copy completed. Refreshing index. This may take some time.")
		if err := esindex.Optimize(pridst, maxsegs); err != nil {
			log.Printf("Error optimizing: %v", err)
			log.Fatalf("Copy completed successfully. Optimize and reenable refreshing manually.")
		}
		log.Printf("Optimize completed. Setting refresh interval to %s", refreshint)

		// update refresh setting
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{RefreshInterval: refreshint}}}
		if err := esindex.Update(pridst, &m); err != nil {
			log.Printf("Error enabling refreshing: %v", err)
			log.Fatalf("Copy completed successfully. Reenable refreshing manually.")
		}
	}

	if delayreplicaton {
		// update refresh setting
		m := esindex.Meta{Settings: &esindex.Settings{Index: &esindex.IndexSettings{Replicas: &replicationfactor}}}
		if err := esindex.Update(pridst, &m); err != nil {
			log.Printf("Error enabling replicas[%v]: %v", replicationfactor, err)
			log.Fatalf("Copy completed successfully. Reenable refreshing manually.")
		}
	}

	desmeta, err = esindex.Get(pridst)
	if err != nil {
		log.Printf("error loading destination index settings. err:%v", err)
	}
	b, err = json.Marshal(desmeta)
	if err != nil {
		log.Printf("error marshalling index settings. err:%v", err)
	}
	log.Printf("Completed: destination index settngs: %v", string(b))
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+msg+"\n", args...)
	flag.Usage()
	os.Exit(2)
}
