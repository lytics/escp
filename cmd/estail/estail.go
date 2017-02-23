package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/lytics/escp/logging"
)

func fatalf(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(2)
}

// StringArray implements flag.Value interface
type StringArray []string

func (a *StringArray) Set(s string) error {
	msgs := strings.Split(s, ",")
	*a = append(*a, msgs...)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

func main() {
	host := "localhost:9200"
	indexPrefix := "logstash-"
	msgFields := StringArray{}
	timeField := "@timestamp"
	include := ""
	exclude := ""
	size := 1000
	poll := 1
	useSSL := false
	useSource := false
	showID := false

	flag.StringVar(&host, "host", host, "host and port of elasticsearch")
	flag.StringVar(&indexPrefix, "prefix", indexPrefix, "prefix of log indexes")
	flag.Var(&msgFields, "message", "message fields to display")
	flag.StringVar(&timeField, "timestamp", timeField, "timestap field to sort by")
	flag.StringVar(&include, "include", include, "comma separated list of field:value pairs to include")
	flag.StringVar(&exclude, "exclude", exclude, "comma separated list of field:value pairs to exclude")
	flag.IntVar(&size, "size", size, "number of docs to return per polling interval")
	flag.IntVar(&poll, "poll", poll, "time in seconds to poll for new data from ES")
	flag.BoolVar(&useSSL, "ssl", useSSL, "use https for URI scheme")
	flag.BoolVar(&useSource, "source", useSource, "use _source field to output result")
	flag.BoolVar(&showID, "id", showID, "show _id field")

	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s  --host=localhost:9401\n", os.Args[0])
		flag.PrintDefaults()
	}

	logger := log.NewStdLogger(true, log.DEBUG, "")

	// If no message field is explicitly requested we will follow @message
	if len(msgFields) == 0 {
		msgFields = append(msgFields, "@message")
	}

	exFilter := map[string]interface{}{}
	if len(include) == 0 && len(exclude) == 0 {
		exFilter["match_all"] = map[string]interface{}{}
	} else {
		filter := map[string]interface{}{}
		if len(include) > 0 {
			filter["must"] = getTerms(include)
		}
		if len(exclude) > 0 {
			filter["must_not"] = getTerms(exclude)
		}
		exFilter["bool"] = filter
	}

	lastTime := time.Now()

	var scheme string
	if useSSL {
		scheme = "https"
	} else {
		scheme = "http"
	}
	rootURL := fmt.Sprintf("%s://%s", scheme, host)

	for {
		resp, err := http.Get(fmt.Sprintf("%s/_status", rootURL))
		if err != nil {
			fatalf("Error contacting Elasticsearch %s: %v", host, err)
		}
		status := map[string]interface{}{}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fatalf("unable to read response body err:%v", err)
		}
		resp.Body.Close()
		if err := json.Unmarshal(body, &status); err != nil {
			fatalf("Error decoding _status response from Elasticsearch: err:%v json:%v", err, string(body))
		}

		logger.Debugf("idx resp: %v", string(body))

		indices := []string{}
		for k, _ := range status["indices"].(map[string]interface{}) {
			if strings.HasPrefix(k, indexPrefix) {
				indices = append(indices, k)
			}
		}
		if len(indices) == 0 {
			fatalf("No indexes found with the prefix '%s'", indexPrefix)
		}
		sort.Strings(indices)
		index := indices[len(indices)-1]

		url := fmt.Sprintf("%s/%s/_search", rootURL, index)
		req, err := json.Marshal(map[string]interface{}{
			"filter": map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							timeField: map[string]interface{}{
								"gt": lastTime.Format(time.RFC3339Nano),
							},
						},
					},
					exFilter,
				},
			},
			"size":    size,
			"_source": useSource,
			"fields":  append(msgFields, timeField),
		})
		if err != nil {
			fatalf("Error creating search body: %v", err)
		}
		resp, err = http.Post(url, "application/json", bytes.NewReader(req))
		if err != nil {
			fatalf("Error searching Elasticsearch: %v", err)
		}
		if resp.StatusCode != 200 {
			body, _ := ioutil.ReadAll(resp.Body)
			fatalf("Elasticsearch failed: %s\n%s", resp.Status, string(body))
		}
		results := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			fatalf("Error reading search results: %v", err)
		}
		resp.Body.Close()

		hits := results["hits"].(map[string]interface{})["hits"].([]interface{})
		for _, hit := range hits {
			h, ok := hit.(map[string]interface{})
			if !ok {
				continue
			}

			fields := h["fields"].(map[string]interface{})
			var target map[string]interface{}
			if useSource {
				target = h["_source"].(map[string]interface{})
			} else {
				target = fields
			}

			output := []string{}
			if showID {
				output = append(output, fmt.Sprintf("id:%v", h["_id"]))
			}

			for _, msgField := range msgFields {
				if v, _ := target[msgField]; v != nil {
					output = append(output, fmt.Sprintf("%s:%v", msgField, v))
				}
			}

			ts := fields[timeField].([]interface{})[0].(string)
			fmt.Printf("%s %s\n", ts, strings.Join(output, "\t"))

			lastTime, err = time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				fatalf("Error decoding timestamp: %v", err)
			}
		}

		time.Sleep(time.Duration(poll) * time.Second)
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
