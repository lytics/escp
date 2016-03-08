package esshards

import (
	"fmt"
	"os"
	"testing"

	"github.com/lytics/escp/estypes"
)

var endpointInfo *estypes.SearchShardsEndpoint

func init() {
	endpointInfo = estypes.NewSearchShards()

	na := make(map[string]estypes.NodeAttributes)
	na["uid1"] = estypes.NodeAttributes{Name: "es1"}
	na["uid2"] = estypes.NodeAttributes{Name: "es2"}
	na["uid3"] = estypes.NodeAttributes{Name: "es3"}
	na["uid4"] = estypes.NodeAttributes{Name: "es4"}
	endpointInfo.Nodes = na

	sattr0 := estypes.ShardAttributes{Primary: true, Shard: 0, Node: "uid1", Index: "index0"}
	sattr1 := estypes.ShardAttributes{Primary: false, Shard: 0, Node: "uid2", Index: "index0"}
	saList := make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)

	sinfo0 := make([]estypes.ShardInfo, 0)
	sinfo0 = append(sinfo0, saList)

	sattr0 = estypes.ShardAttributes{Primary: true, Shard: 1, Node: "uid2", Index: "index0"}
	sattr1 = estypes.ShardAttributes{Primary: false, Shard: 1, Node: "uid3", Index: "index0"}
	saList = make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)
	sinfo0 = append(sinfo0, saList)

	sattr0 = estypes.ShardAttributes{Primary: true, Shard: 2, Node: "uid3", Index: "index0"}
	sattr1 = estypes.ShardAttributes{Primary: false, Shard: 2, Node: "uid1", Index: "index0"}
	saList = make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)
	sinfo0 = append(sinfo0, saList)

	//Index1
	sattr0 = estypes.ShardAttributes{Primary: true, Shard: 0, Node: "uid4", Index: "index1"}
	sattr1 = estypes.ShardAttributes{Primary: false, Shard: 0, Node: "uid2", Index: "index1"}
	saList = make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)
	sinfo0 = append(sinfo0, saList)

	sattr0 = estypes.ShardAttributes{Primary: true, Shard: 1, Node: "uid2", Index: "index1"}
	sattr1 = estypes.ShardAttributes{Primary: false, Shard: 1, Node: "uid4", Index: "index1"}
	saList = make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)
	sinfo0 = append(sinfo0, saList)

	sattr0 = estypes.ShardAttributes{Primary: false, Shard: 2, Node: "uid1", Index: "index1"}
	sattr1 = estypes.ShardAttributes{Primary: true, Shard: 2, Node: "uid4", Index: "index1"}
	saList = make([]estypes.ShardAttributes, 0)
	saList = append(saList, sattr0)
	saList = append(saList, sattr1)
	sinfo0 = append(sinfo0, saList)
	endpointInfo.Shards = sinfo0
}

func TestMatching(t *testing.T) {

	x := map[string]struct{}{"hi": struct{}{}, "cat": struct{}{}}
	y := map[string]struct{}{"cat": struct{}{}, "neh": struct{}{}}

	match := MatchMaps(x, y)
	if len(match) > 1 {
		t.Errorf("Match size too large: %#v\n", match)
	}
	if _, ex := match["cat"]; !ex {
		t.Errorf("Error matching common elements of set")
	}
}

func TestCommonNodes(t *testing.T) {
	nodes := make(map[string]string)
	nodes["uid1"] = "es1"
	nodes["uid2"] = "es2"
	nodes["uid3"] = "es3"
	cmi := CommonPrimaryIndexes(endpointInfo, nodes)

	if _, e := cmi["index0"]; !e {
		t.Errorf("index0 primary shards not matched between uids[1,2,3]")
	}

	nodes = make(map[string]string)
	nodes["uid2"] = "es2"
	nodes["uid4"] = "es4"
	cmi = CommonPrimaryIndexes(endpointInfo, nodes)

	if _, e := cmi["index1"]; !e {
		t.Errorf("index1 primary shards not matched between uids[2,4]")
	}
}

func TestNodesHRNames(t *testing.T) {
	HRNodes := make(map[string]struct{})
	HRNodes["es1"] = struct{}{}
	HRNodes["es4"] = struct{}{}

	ids := NodesFromHRName(*endpointInfo, HRNodes)
	if _, e := ids["uid4"]; !e {
		t.Error("'uid4' not returned from human readable node names")
	}
	if _, e := ids["uid1"]; !e {
		t.Error("'uid1' not returned from human readable node names")
	}
}

func TestPrimariesPerNode(t *testing.T) {
	primaryNodes := make(map[string]map[string]struct{})
	info := endpointInfo

	nodeids := NodeIDs(*info)
	shardAttrs := FlatShardAttributes(info.Shards)
	primeMap := ProcessShardList(shardAttrs, nodeids)

	type empty struct{}
	for pk, pv := range primeMap {
		for _, iv := range pv {
			for _, s := range iv {
				//fmt.Printf("\t\t%s; %v\n", s.Index, s.Shard)
				if _, e := primaryNodes[pk]; !e {
					primaryNodes[pk] = make(map[string]struct{})
				}
				if _, e := primaryNodes[pk][s.Index]; !e {
					primaryNodes[pk][s.Index] = empty{}
				}
			}
		}
	}
	if _, e := primaryNodes["uid4"]["index1"]; !e {
		t.Error("Primary shard of index1 not on uid4")
	}

	_, uid2_i0 := primaryNodes["uid2"]["index0"]
	_, uid2_i1 := primaryNodes["uid2"]["index1"]
	if !uid2_i0 || !uid2_i1 {
		t.Error("Both indexes should have primary shards on uid2")
	}

	/*
		for k, _ := range primaryNodes {
			fmt.Printf("%s\n", k)
			for ki, _ := range primaryNodes[k] {
				fmt.Printf("\t%s\n", ki)
			}
		}
	*/

}

//Query a local ES node and display all the indexes
// Skipped unless TESTINT=1 is set in env
func TestFullIntegration(t *testing.T) {
	//Skip visual data inspection if TESTINT environment variable isn't set to "1"
	if s := os.Getenv("TESTINT"); s != "1" {
		return
	}
	info, err := Get("http://localhost:9200/_search_shards")
	if err != nil {
		t.Error(err)
	}

	nodeids := NodeIDs(*info)
	shardAttrs := FlatShardAttributes(info.Shards)

	primeMap := ProcessShardList(shardAttrs, nodeids)
	for pk, pv := range primeMap {
		fmt.Printf("Node: %s\n", pk)
		for ik, iv := range pv {
			//fmt.Printf("\tIndex: %s\n%#v\n", ik, iv)
			fmt.Printf("\tIndex: %s\n", ik)
			for _, s := range iv {
				fmt.Printf("\t\t%s; %v\n", s.Index, s.Shard)
			}
		}
	}
}
