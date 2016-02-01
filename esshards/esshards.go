package esshards

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/lytics/escp/estypes"
)

var ErrMissing = errors.New("Error GETting _search_shards endpoint")

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
