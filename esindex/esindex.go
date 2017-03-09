package esindex

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
	Replicas        *int              `json:"number_of_replicas,string,omitempty"`
	Shards          *int              `json:"number_of_shards,string,omitempty"`
	RefreshInterval string            `json:"refresh_interval,omitempty"`
	CompoundOnFlush bool              `json:"compound_on_flush,omitempty"`
	CompoundFormat  bool              `json:"compound_format,omitempty"`
	Mapping         *IndexMapping     `json:"mapping,omitempty"`
	Unassigned      *UnassignedWarper `json:"unassigned,omitempty"`
}

type IndexMapping struct {
	NestedFields *FieldsSetting `json:"nested_fields,omitempty"`
}

type FieldsSetting struct {
	Limit int `json:"limit,string,omitempty"`
}

type UnassignedWarper struct {
	NodeOption *NodeOptions `json:"node_left,omitempty"`
}

type NodeOptions struct {
	DelayTimeout string `json:"delayed_timeout,omitempty"`
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
		return nil, fmt.Errorf("Get::Uri:%v err:%v", dst, err)
	}
	if resp.StatusCode == 404 {
		return nil, ErrMissing
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Get::Uri:%v Non-200 status code from source Elasticsearch: %d", dst, resp.StatusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get::Error reading response: %v", err)
	}
	idxmetamap := make(map[string]*Meta, 1)
	if err := json.Unmarshal(b, &idxmetamap); err != nil {
		return nil, fmt.Errorf("Get::error decoding response: err:%v body:%v", err, string(b))
	}

	parts := strings.Split(dst, "/")
	idxname := parts[len(parts)-1]
	idxmeta, ok := idxmetamap[idxname]
	if !ok {
		return nil, fmt.Errorf("Get:: index %s not found", idxname)
	}
	// Shards should always be set, so use this as an indicator things didn't get
	// unmarshalled properly.
	if idxmeta.Settings.Index.Shards == nil {
		return nil, fmt.Errorf("Get::unable to read existing shards for index %s", idxname)
	}
	return idxmeta, nil
}

func GetDocCount(idx string) (uint64, error) {
	hresp, err := http.Get(idx + "/_search?size=0")
	if err != nil {
		return 0, fmt.Errorf("error contacting index:%v err:%v", idx, err)
	}
	newres := estypes.Results{}
	if err := json.NewDecoder(hresp.Body).Decode(&newres); err != nil {
		return 0, fmt.Errorf("error reading target index:%v err:%v", idx, err)
	}
	return newres.Hits.Total, nil
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
