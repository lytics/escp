package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"encoding/json"

	"github.com/lytics/escp/esscroll"
	log "github.com/lytics/escp/logging"
)

func fatalf(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(2)
}

func main() {
	host := "localhost:9200"
	indexPrefix := "logstash-2017.02.23"
	timeField := "@timestamp"
	include := ""
	exclude := ""
	size := 1000
	//poll := 1
	useSSL := false
	timeSpan := 30 * time.Second

	flag.StringVar(&host, "host", host, "host and port of elasticsearch")
	flag.StringVar(&indexPrefix, "prefix", indexPrefix, "prefix of log indexes")
	flag.StringVar(&timeField, "timestamp", timeField, "timestap field to sort by")
	flag.StringVar(&include, "include", include, "DOESNT WORK: comma separated list of field:value pairs to include")
	flag.StringVar(&exclude, "exclude", exclude, "DOESNT WORK: comma separated list of field:value pairs to exclude")
	flag.IntVar(&size, "size", size, "number of docs to return per polling interval")
	//flag.IntVar(&poll, "poll", poll, "time in seconds to poll for new data from ES")
	flag.BoolVar(&useSSL, "ssl", useSSL, "use https for URI scheme")
	flag.DurationVar(&timeSpan, "dur", timeSpan, "now() - dur are how many logs are pulled")

	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage of %s --host=localhost:9401 --dur=10s  # pull the latest 10 seconds of logs `, os.Args[0], os.Args[0])
		flag.PrintDefaults()
		return
	}

	logger := log.NewStdLogger(true, log.DEBUG, "")

	var scheme string
	if useSSL {
		scheme = "https"
	} else {
		scheme = "http"
	}
	rootURL := fmt.Sprintf("%s://%s", scheme, host)

	docValueFilter := map[string]interface{}{}
	if len(include) == 0 && len(exclude) == 0 {
		docValueFilter["match_all"] = map[string]interface{}{}
	} else {
		filter := map[string]interface{}{}
		if len(include) > 0 {
			filter["must"] = getTerms(include)
		}
		if len(exclude) > 0 {
			filter["must_not"] = getTerms(exclude)
		}
		docValueFilter["bool"] = filter
	}
	timeRangeFilter := map[string]interface{}{
		"range": map[string]interface{}{
			timeField: map[string]interface{}{
				"gt": time.Now().Add(-1 * timeSpan).Format(time.RFC3339Nano),
			},
		},
	}

	filter := map[string]interface{}{
		"and": []interface{}{
			timeRangeFilter,
			docValueFilter,
		},
	}

	scanURL := fmt.Sprintf("%s/%s", rootURL, indexPrefix)
	// Start the scroll first to make sure the source parameter is valid
	ess := esscroll.New(context.Background(), scanURL, time.Minute, size, 3, filter, 10*time.Minute, logger)
	resp, err := ess.Start()
	if err != nil {
		fatalf("%v", err)
	}

	b, _ := json.Marshal(filter)
	logger.Infof("Scrolling over %d documents from %v : filter:%v\n", resp.Total, rootURL, string(b))

	for doc := range resp.Hits {
		b, err := doc.Source.MarshalJSON()
		if err != nil {
			fatalf("%v", err)
		}
		fmt.Printf("%s\n", string(b))
	}
}

// split string and parse to terms for query filter
func getTerms(args string) []map[string]interface{} {
	terms := []map[string]interface{}{}
	for k, v := range parsePairs(args) {
		terms = append(terms, map[string]interface{}{"terms": map[string]interface{}{k: v}})
	}
	return terms
}

// split string and parse to key-value pairs
func parsePairs(args string) map[string][]string {
	exkv := map[string][]string{}
	for _, pair := range strings.Split(args, ",") {

		kv := strings.Split(pair, ":")
		if _, ok := exkv[kv[0]]; ok {
			exkv[kv[0]] = append(exkv[kv[0]], kv[1])
		} else {
			exkv[kv[0]] = []string{kv[1]}
		}
	}
	return exkv
}
