package esbulk

import (
	"encoding/json"

	"github.com/lytics/escp/estypes"
)

type BulkAction struct {
	Index *estypes.Meta `json:"index,omitempty"`
}

/*
{
   "took": 3,
   "errors": true,
   "items": [
      {  "create": {
            "_index":   "website",
            "_type":    "blog",
            "_id":      "123",
            "status":   409,
            "error":    "DocumentAlreadyExistsException
                        [[website][4] [blog][123]:
                        document already exists]"
      }},
      {  "index": {
            "_index":   "website",
            "_type":    "blog",
            "_id":      "123",
            "_version": 5,
            "status":   200
      }}
   ]
}
*/
type BulkResponse struct {
	Errors bool `json:"errors"`

	// Items is a RawMessage so you can skip parsing if Errors=false
	Items *json.RawMessage `json:"items"`
}

type BulkActionResponseItem struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
	Status  int    `json:"status"`
	Error   string `json:"error"`
}
