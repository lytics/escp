package esbulk

import "github.com/lytics/escp/estypes"

type BulkAction struct {
	Index *estypes.Meta `json:"index,omitempty"`
}

type BulkResponse struct {
	Errors bool `json:"errors"`
	//Items []*BulkActionResponse //TODO
}
