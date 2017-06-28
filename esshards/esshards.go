package esshards

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/lytics/escp/estypes"
)

var ErrMissing = errors.New("Error GETting _search_shards endpoint")

// Get returns structured data from the _search_shards endpoint.
// This includes information about the cluster's nodes and flat listing of shards.
func Get(dst string) (*estypes.SearchShardsEndpoint, error) {
	resp, err := http.Get(dst)
	if err != nil {
		return nil, fmt.Errorf("error contacting source Elasticsearch: %v", err)
	}

	if resp.StatusCode == 404 {
		return nil, ErrMissing
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("non-200 status code from source Elasticsearch: %d", resp.StatusCode)
	}

	shardInfo := estypes.NewSearchShards()
	if err := json.NewDecoder(resp.Body).Decode(&shardInfo); err != nil {
		return nil, err
	}
	return shardInfo, nil
}

//Return a map of ES node IDs with built map sets for shard IDs
func NodeIDs(endpoint estypes.SearchShardsEndpoint) map[string]map[string][]estypes.ShardAttributes {
	nodemap := make(map[string]map[string][]estypes.ShardAttributes)

	for k, _ := range endpoint.Nodes {
		nodemap[k] = make(map[string][]estypes.ShardAttributes)
	}
	return nodemap
}

// NodesFromHRName matches a list human-readable node names and returns a list of InternalIDs.
func NodesFromHRName(endpoint estypes.SearchShardsEndpoint, esNames map[string]struct{}) map[string]string {
	matching := make(map[string]string)
	for k, v := range endpoint.Nodes {
		if _, e := esNames[v.Name]; e {
			matching[k] = v.Name
		}
	}
	return matching
}

// PrimaryShards accepts a list of ShardAttributes filter and return the Primary shards
func PrimaryShards(shards []estypes.ShardAttributes) []estypes.ShardAttributes {
	primaries := make([]estypes.ShardAttributes, 0, 0)

	for _, v := range shards {
		if v.Primary == true {
			primaries = append(primaries, v)
		}
	}
	return primaries
}

// FlatShardAttributes accepts a slice ShardInfo([]ShardAttributes), and
// unpacks all of the contained ShardAttributes into a new list to return.
func FlatShardAttributes(shardList []estypes.ShardInfo) []estypes.ShardAttributes {
	shardAttrs := make([]estypes.ShardAttributes, 0, 0)
	for _, si := range shardList {
		for _, s := range si {
			shardAttrs = append(shardAttrs, s)
		}
	}
	return shardAttrs
}

// ProcessShardList calculates a nested map[Node][Index][]ShardAttributes
// which represents [NodeName][IndexName][]PrimaryShards and is returned.
// Exposing which Nodes host an Index's Primary shard.
func ProcessShardList(shardList []estypes.ShardAttributes, nodemap map[string]map[string][]estypes.ShardAttributes) map[string]map[string][]estypes.ShardAttributes {
	primaries := PrimaryShards(shardList)

	for _, p := range primaries {
		nodemap[p.Node][p.Index] = append(nodemap[p.Node][p.Index], p)
	}
	return nodemap
}

// Discover the primary indexes owned by ES Nodes
func NodeIndexSets(info estypes.SearchShardsEndpoint) map[string]map[string]struct{} {

	primaryNodes := make(map[string]map[string]struct{})

	nodeids := NodeIDs(info)
	shardAttrs := FlatShardAttributes(info.Shards)
	primeMap := ProcessShardList(shardAttrs, nodeids)

	type empty struct{}
	for pk, pv := range primeMap {
		for _, iv := range pv {
			for _, s := range iv {
				//Create map for node if new
				if _, e := primaryNodes[pk]; !e {
					primaryNodes[pk] = make(map[string]struct{})
				}
				//Assign index to Node's set
				if _, e := primaryNodes[pk][s.Index]; !e {
					primaryNodes[pk][s.Index] = empty{}
				}
			}
		}
	}
	return primaryNodes
}

// CommonPrimaryIndexes builds on NodeIndexSets(...) to produce a set of Indexes common to
// a set of Nodes.
func CommonPrimaryIndexes(info *estypes.SearchShardsEndpoint, nodeIDs map[string]string) map[string]struct{} {
	commonIndexes := make(map[string]struct{})
	nodeSets := NodeIndexSets(*info)

	for nk, nv := range nodeSets {
		if _, exists := nodeIDs[nk]; exists {
			if len(commonIndexes) == 0 {
				commonIndexes = nv
			} else {
				commonIndexes = MatchMaps(nv, commonIndexes)
			}
		}
	}
	return commonIndexes
}

// MatchMaps compares two map sets and returns the common keys
func MatchMaps(x, y map[string]struct{}) map[string]struct{} {
	z := make(map[string]struct{})

	for k, _ := range x {
		if _, exists := y[k]; exists {
			z[k] = struct{}{}
		}
	}
	return z
}
