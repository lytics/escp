package esindex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/lytics/escp/estypes"
)

// Metadata describing an Elasticsearch index.
type Meta struct {
	Settings *Settings `json:"settings"`
}

// Settings for an Elasticsearch index.
type Settings struct {
	Index *IndexSettings `json:"index"`
}

type IndexSettings struct {
	Replicas        *int   `json:"number_of_replicas,string,omitempty"`
	Shards          *int   `json:"number_of_shards,string,omitempty"`
	RefreshInterval string `json:"refresh_interval,omitempty"`
}

var (
	ErrMissing = errors.New("index missing")
	ErrExists  = errors.New("index exists")
)

// Create an index with the specified metadata. Returns ErrExists if the index
// already exists.
func Create(dst string, m *Meta) error {
	// Make sure the index doesn't already exist first
	existing, err := Get(dst)
	if err != nil && err != ErrMissing {
		return fmt.Errorf("error checking for existing index: %v", err)
	}
	if existing != nil {
		return ErrExists
	}

	return put(dst, m)
}

// Get metadata about an index. Returns ErrMissing if index doesn't existing.
func Get(dst string) (*Meta, error) {
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

	idxmetamap := make(map[string]*Meta, 1)
	if err := json.NewDecoder(resp.Body).Decode(&idxmetamap); err != nil {
		return nil, fmt.Errorf("error decoding index metadata: %v", err)
	}
	parts := strings.Split(dst, "/")
	idxname := parts[len(parts)-1]
	idxmeta, ok := idxmetamap[idxname]
	if !ok {
		return nil, fmt.Errorf("index %s not found", idxname)
	}
	// Shards should always be set, so use this as an indicator things didn't get
	// unmarshalled properly.
	if idxmeta.Settings.Index.Shards == nil {
		return nil, fmt.Errorf("unable to read existing shards for index %s", idxname)
	}
	return idxmeta, nil
}

// Update index metadata
func Update(dst string, m *Meta) error {
	return put(dst+"/_settings", m)
}

func put(dst string, m *Meta) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("error encoding index json: %v", err)
	}
	req, err := http.NewRequest("PUT", dst, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("error creating index request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error creating index %s: %v", dst, err)
	}
	ackr := estypes.AckResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&ackr); err != nil {
		return fmt.Errorf("error decoding index response: %v", err)
	}
	if !ackr.Ack {
		return estypes.ErrUnack
	}
	return nil
}