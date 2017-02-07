package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"net/url"

	"github.com/lytics/escp/jobs"
	log "github.com/lytics/escp/logging"
)

func main() {
	logger := log.NewStdLogger(true, log.DEBUG, "")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://SRCHOST1:9200 INDEX1 DESHOST2:9200,DESHOST3:9200,DESHOST4:9200 INDEX2\n", os.Args[0])
		flag.PrintDefaults()
	}

	// Index creation settings
	shards := 0
	flag.IntVar(&shards, "shards", shards, "number of shards target index will have (default = same as old index)")
	skipcreate := false
	flag.BoolVar(&skipcreate, "skipcreate", skipcreate, "skip destination index creation")

	// Tunables
	scrolltimeout := 15 * time.Minute
	flag.DurationVar(&scrolltimeout, "scrolltime", scrolltimeout, "time to keep scroll alive between requests")
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

	refreshint := time.Duration(0)
	flag.DurationVar(&refreshint, "refreshint", refreshint, "if indexing is delayed, what to set the refresh interval to after copy; defaults to old index's setting or 1s")
	maxsegs := 5
	flag.IntVar(&maxsegs, "maxsegs", maxsegs, "if indexing is delayed, the max number of segments for the optimized index")
	createdelay := time.Second
	flag.DurationVar(&createdelay, "createdelay", createdelay, "time to sleep after index creation to let cluster go green")

	logevery := 10 * time.Minute
	flag.DurationVar(&logevery, "logevery", logevery, "rate at which to log progress metrics.")

	flag.Parse()

	if bulksz != 128*1024 {
		bulksz = bulksz * 1024
	}

	if flag.NArg() != 4 {
		logger.Errorf("expected 3 arguments, found %d\n", flag.NArg())
		flag.Usage()
		os.Exit(1)
	}
	if shards > 0 && skipcreate {
		logger.Errorf("cannot set shards and skip index creation")
		flag.Usage()
		os.Exit(1)
	}

	src, err := jobs.ParseUrl(flag.Arg(0))
	if err != nil {
		logger.Errorf("error parsing url:%v err:%v", flag.Arg(0), err)
		os.Exit(1)
	}
	srcIdx := flag.Arg(1)
	if strings.HasSuffix(srcIdx, "/") {
		srcIdx = srcIdx[:len(srcIdx)-1]
	}

	dstsR := strings.Split(flag.Arg(2), ",")
	if len(dstsR) < 1 {
		logger.Errorf("need at least one destination host")
		flag.Usage()
		os.Exit(1)
	}
	dsts := []*url.URL{}
	for _, u := range dstsR {
		d, err := jobs.ParseUrl(u)
		if err != nil {
			logger.Errorf("error parsing url:%v err:%v", u, err)
			os.Exit(1)
		}
		dsts = append(dsts, d)
	}
	desidx := flag.Arg(3)

	if bulkpar == 0 {
		bulkpar = len(dsts) * 2
	}

	srcC := &jobs.SourceConfig{
		IndexName:     srcIdx,
		Host:          src,
		ScrollTimeout: scrolltimeout,
		ScrollPage:    scrollpage,
		ScrollDocs:    scrolldocs,
		Filter:        nil,
	}
	desC := &jobs.DesConfig{
		IndexName:         desidx,
		Hosts:             dsts,
		CreateDelay:       createdelay,
		RefreshInt:        refreshint,
		Shards:            shards,
		DelayRefresh:      delayrefresh,
		SkipCreate:        skipcreate,
		DelayReplicaton:   delayreplicaton,
		ReplicationFactor: replicationfactor,
		MaxSeg:            maxsegs,
		BulkSize:          bulksz,
		NumWorkers:        bulkpar,
	}

	if err := jobs.Copy(context.Background(), srcC, desC, logger, logevery); err != nil {
		logger.Errorf("%v", err)
		os.Exit(1)
	}

}
