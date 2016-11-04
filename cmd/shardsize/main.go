package main

import (
	"flag"
	"sort"

	log "github.com/Sirupsen/logrus"
	"github.com/lytics/escp/esshards"
	"github.com/lytics/escp/esstats"
	"github.com/lytics/escp/estypes"
	"github.com/pivotal-golang/bytefmt"
)

func main() {
	var hostAddr string
	log.SetLevel(log.InfoLevel)

	flag.StringVar(&hostAddr, "host", "http://localhost:9200/", "Elasticsearch query address")
	flag.Parse()

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
		ii.CalculateShards()
		indices[k] = ii
		indexList = append(indexList, ii)
	}

	sort.Sort(estypes.IndexSort(indexList))
	log.Infof("         Index          Size   Shards (Optimal Shards)")
	for _, sc := range indexList {
		if sc.OptimalShards-sc.ShardCount > 10 {
			log.Infof("%20s: %7s  %3d:%8d", sc.Name, bytefmt.ByteSize(uint64(sc.ByteSize)), sc.ShardCount, sc.OptimalShards)
		}
	}
}
