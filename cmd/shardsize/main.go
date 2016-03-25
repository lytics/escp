package main

import (
	"flag"
	"sort"

	log "github.com/Sirupsen/logrus"
	"github.com/lytics/escp/esshards"
	"github.com/lytics/escp/esstats"
	"github.com/lytics/escp/estypes"
)

func main() {
	var hostAddr string
	var nodesRaw string
	log.SetLevel(log.InfoLevel)

	flag.StringVar(&nodesRaw, "nodes", "", "Nodes to find common primary shards eg: \"es1,es2,es3\"")
	flag.StringVar(&hostAddr, "host", "http://localhost:9200/", "Elasticsearch query address")
	flag.Parse()

	log.Debugf("nodesRaw: %v", nodesRaw)
	//nodes = strings.Split(nodesRaw, ",")

	shardAddr := hostAddr + "_stats"
	stats, err := esstats.Get(shardAddr)
	if err != nil {
		log.Fatalf("Error querying shard info from Elasticsearch:\n%#v", err)
	}

	indices := make(map[string]estypes.IndexInfo)
	for k, v := range stats.Indices {
		indices[k] = estypes.IndexInfo{Name: k, ByteSize: v.Primaries.Store.IndexByteSize}
	}

	shardInfo, err := esshards.Get(hostAddr + "_search_shards")
	if err != nil {
		log.Fatalf("Error querying shard info from Elasticsearch:\n%#v", err)
	}

	shardCount := esstats.CountShards(shardInfo.Shards)

	indexList := make([]estypes.IndexInfo, 0)
	for k, v := range shardCount {
		ii := indices[k]
		ii.ShardCount = v
		indices[k] = ii
		indexList = append(indexList, ii)
	}

	sort.Sort(estypes.IndexSort(indexList))
	for i, sc := range indexList {
		log.Infof("%d: %#v", i, sc)
	}

}
