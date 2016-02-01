package estypes

import (
	"encoding/json"
	"errors"
)

type Meta struct {
	ID    string `json:"_id"`
	Type  string `json:"_type"`
	Index string `json:"_index"`
	//Version string `json:"_version"` //FIXME _version not in _search results?!
}

type Doc struct {
	Meta
	Source json.RawMessage `json:"_source,omitempty"`
}

type Hits struct {
	Hits  []*Doc `json:"hits"`
	Total uint64 `json:"total"`
}

type Results struct {
	Hits     *Hits  `json:"hits"`
	TimedOut bool   `json:"timed_out"`
	ScrollID string `json:"_scroll_id"`
}

type AckResponse struct {
	Ack bool `json:"acknowledged"`
}

// ErrFailed should be returned any time Elasticsearch returns
// acknowledged=false.
var ErrUnack = errors.New("request unacknowledged")

// _search_shards endpoint data
type SearchShardsEndpoint struct {
	Nodes  NodeInfo  `json:"nodes"`
	Shards ShardList `json:"shards"`
}

func NewSearchShards() *SearchShardsEndpoint {
	Nodes := make(map[string]NodeAttributes)
	Shards := make(ShardList, 0)
	return &SearchShardsEndpoint{Nodes, Shards}
}

type NodeInfo map[string]NodeAttributes

type NodeAttributes struct {
	Name             string `json:"name"`
	TransportAddress string `json:"transport_address"`
}

type ShardList []ShardInfo

type ShardInfo []ShardAttributes

type ShardAttributes struct {
	State   string `json:"state"`
	Primary bool   `json:"primary"`
	Node    string `json:"node"`
	Relocating bool   `json:"relocating_node"`
	Shard int    `json:"shard"`
	Index string `json:"index"`
}

/*
 Structs for the /_stats endpoint
*/
type Stats struct {
	All     StatsAll                `json:"_all"`
	Shards  StatsShards             `json:"_shards"`
	Indices map[string]StatsIndices `json:"indices"`
}

type StatsAll struct{}
type StatsShards struct{}

type StatsIndices struct {
	Primaries IndexPrimary `json:"primaries"`
	//Totals    IndexTotal   `json:"total"`
}

// Index Primary Data
type IndexPrimary struct {
	Store IndexStore `json:"store"`
}

type IndexStore struct {
	IndexByteSize int `json:"size_in_bytes"`
}

type IndexInfo struct {
	Name          string
	ByteSize      int
	ShardCount    int
	BytesPerShard int
}

type IndexSort []IndexInfo

func (is IndexSort) Len() int      { return len(is) }
func (is IndexSort) Swap(i, j int) { is[i], is[j] = is[j], is[i] }
func (is IndexSort) Less(i, j int) bool {
	if is[i].BytesPerShard == 0 {
		is[i].BytesPerShard = is[i].ByteSize / is[i].ShardCount
	}
	if is[j].BytesPerShard == 0 {
		is[j].BytesPerShard = is[j].ByteSize / is[j].ShardCount
	}
	return is[i].BytesPerShard < is[j].BytesPerShard
}
