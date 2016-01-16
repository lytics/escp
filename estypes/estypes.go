package estypes

import "encoding/json"

type Meta struct {
	ID    string `json:"_id"`
	Type  string `json:"_type"`
	Index string `json:"_index"`
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
