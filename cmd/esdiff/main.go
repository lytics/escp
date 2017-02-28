package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lytics/escp/jobs"
	log "github.com/lytics/escp/logging"
)

func main() {
	logger := log.NewStdLogger(true, log.DEBUG, "")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://host1:9200 index1 http://host2:9200 index2\n", os.Args[0])
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
	logevery := 10 * time.Minute
	flag.DurationVar(&logevery, "logevery", logevery, "rate at which to log progress metrics.")

	flag.Parse()
	if flag.NArg() != 4 {
		fatalf("requires 2 arguments")
	}

	surl, err := jobs.ParseUrl(flag.Arg(0))
	if err != nil {
		logger.Errorf("error parsing url:%v err:%v", flag.Arg(0), err)
		os.Exit(1)
	}
	srcIdx := flag.Arg(1)
	if strings.HasSuffix(srcIdx, "/") {
		srcIdx = srcIdx[:len(srcIdx)-1]
	}

	durl, err := jobs.ParseUrl(flag.Arg(2))
	if err != nil {
		logger.Errorf("error parsing url:%v err:%v", flag.Arg(2), err)
		os.Exit(1)
	}
	dstIdx := flag.Arg(3)
	if strings.HasSuffix(dstIdx, "/") {
		dstIdx = dstIdx[:len(dstIdx)-1]
	}

	if denom < 2 {
		denom = 1
	}

	srcC := &jobs.SourceConfig{
		IndexName:     srcIdx,
		Host:          surl,
		ScrollTimeout: time.Minute,
		ScrollPage:    1000,
		ScrollDocs:    1,
	}

	desC := &jobs.DesConfig{
		IndexName: dstIdx,
		Hosts:     []*url.URL{durl},
	}

	vr, err := jobs.Validate(context.Background(), srcC, desC, denom, logger, logevery)
	//logger.Errorf("?: %v  %v", problems, err)
	if err == jobs.ErrMissMatch {
		logger.Errorf("MissMatch: %v", vr)
	} else if err != nil {
		logger.Errorf("validation failed with error:%v", err)
	} else {
		logger.Infof("results:%v", vr)
	}
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "fatal error: "+msg+"\n", args...)
	flag.Usage()
	os.Exit(2)
}
