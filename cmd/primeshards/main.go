package main

import (
	"flag"
	"sort"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/lytics/escp/esshards"
)

func main() {
	var hostAddr string
	var nodes []string
	var nodesRaw string
	log.SetLevel(log.InfoLevel)

	flag.StringVar(&nodesRaw, "nodes", "", "Nodes to find common primary shards eg: \"es1,es2,es3\"")
	flag.StringVar(&hostAddr, "host", "http://localhost:9200/", "Elasticsearch query address")
	flag.Parse()

	log.Debugf("nodesRaw: %v", nodesRaw)
	nodes = strings.Split(nodesRaw, ",")

	shardAddr := hostAddr + "_search_shards"
	info, err := esshards.Get(shardAddr)
	if err != nil {
		log.Fatalf("Error querying shard info from Elasticsearch:\n%#v", err)
	}

	HRnodeSet := make(map[string]struct{})
	for _, v := range nodes {
		HRnodeSet[v] = struct{}{}
	}
	nodeIDs := esshards.NodesFromHRName(*info, HRnodeSet)
	log.Debugf("Nodes associated: %#v", nodeIDs)

	cmi := esshards.CommonPrimaryIndexes(info, nodeIDs)
	log.Infof("Indices with primary shards common to %v:\n", nodeIDs)
	indexes := make([]string, 0)
	for k, _ := range cmi {
		indexes = append(indexes, k)
	}
	sort.Strings(indexes)
	for _, v := range indexes {
		log.Infof("%s", v)
	}
}
