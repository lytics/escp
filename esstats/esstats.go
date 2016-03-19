package esstats

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/lytics/escp/estypes"
)

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
