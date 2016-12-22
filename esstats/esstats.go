package esstats

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/lytics/escp/estypes"
)

// Get and decode the response from _stats debug endpoint.
func Get(qry string) (*estypes.Stats, error) {
	resp, err := http.Get(qry)
	if err != nil {
		return nil, fmt.Errorf("error contacting source Elasticsearch: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non-200 status code from source Elasticsearch: %d", resp.StatusCode)
	}

	statsInfo := &estypes.Stats{Indices: make(map[string]estypes.StatsIndices)}

	if err := json.NewDecoder(resp.Body).Decode(&statsInfo); err != nil {
		return nil, err
	}
	return statsInfo, nil
}

//Create a map of index name to number of shards. Function iterates over
//list of shards returned by _stats and counts them to the index name
func CountShards(shards estypes.ShardList) map[string]int {
	shardCount := make(map[string]int)
	for _, s := range shards {
		if _, ok := shardCount[s[0].Index]; !ok {
			shardCount[s[0].Index] = 1
		} else {
			shardCount[s[0].Index]++
		}
	}
	return shardCount
}
