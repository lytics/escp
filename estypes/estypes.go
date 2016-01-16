package estypes

import "encoding/json"

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

type BulkAction struct {
	Index *Meta `json:"index,omitempty"`
}

type BulkResponse struct {
	Errors bool `json:"errors"`
	//Items []*BulkActionResponse //TODO
}
