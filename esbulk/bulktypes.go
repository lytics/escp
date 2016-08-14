package esbulk

import (
	"bytes"
	"encoding/json"

	"github.com/lytics/escp/estypes"
)

type BulkAction struct {
	Index *estypes.Meta `json:"index,omitempty"`
}

func NewBatch() *Batch {
	return &Batch{
		docs: make(map[string]*estypes.Doc),
		buf:  bytes.NewBuffer(make([]byte, 0, 256000)),
	}
}

type Batch struct {
	buf  *bytes.Buffer
	docs map[string]*estypes.Doc
}

func (b Batch) Reset() {
	b.buf.Reset()
	b.docs = make(map[string]*estypes.Doc)
}

func (b Batch) Add(id string, doc *estypes.Doc) {
	b.docs[id] = doc
}

func (b Batch) Delete(id string) {
	delete(b.docs, id)
}

func (b Batch) Len() int {
	return len(b.docs)
}

func (b Batch) ByteLen() int {
	totallen := 0
	for _, bm := range b.docs {
		totallen += len(bm.Source)
	}
	return totallen
}

func (b Batch) Encode(index string) ([]byte, error) {
	enc := json.NewEncoder(b.buf)
	for _, doc := range b.docs {
		// Write action
		action := BulkAction{}
		action.Index = &doc.Meta
		action.Index.Index = index
		if err := enc.Encode(&action); err != nil {
			return nil, err
		}
		// Write document
		if err := enc.Encode(&doc.Source); err != nil {
			return nil, err
		}
	}
	bs := b.buf.Bytes()
	b.buf.Reset()
	return bs, nil
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
// BulkResponses response from ES's bulk endpoint
type BulkResponses struct {
	Took        int                        `json:"took,omitempty"`
	HasErrors   bool                       `json:"errors,omitempty"`
	Items       []map[string]*BulkResponse `json:"items,omitempty"`
	orginalBody []byte
}

// Failed returns those items of a bulk response that have errors,
// i.e. those that don't have a status code between 200 and 299.
func (r *BulkResponses) Failed(exclude404 bool) []*BulkResponse {
	if r.Items == nil {
		return nil
	}
	errors := make([]*BulkResponse, 0)
	for _, item := range r.Items {
		for _, result := range item {
			if exclude404 && result.Status == 404 {
				continue
			}
			if !(result.Status >= 200 && result.Status <= 299) {
				errors = append(errors, result)
			}
		}
	}
	return errors
}

// Succeeded returns those items of a bulk response that have no errors,
// i.e. those have a status code between 200 and 299.
func (r *BulkResponses) Succeeded(include404 bool) []*BulkResponse {
	if r.Items == nil {
		return nil
	}
	succeeded := make([]*BulkResponse, 0)
	for _, item := range r.Items {
		for _, result := range item {
			if result.Status >= 200 && result.Status <= 299 {
				succeeded = append(succeeded, result)
			}
			if include404 && result.Status == 404 {
				succeeded = append(succeeded, result)
			}
		}
	}
	return succeeded
}

type BulkResponse struct {
	Index   string           `json:"_index,omitempty"`
	Type    string           `json:"_type,omitempty"`
	Id      string           `json:"_id,omitempty"`
	Version int              `json:"_version,omitempty"`
	Shards  *ESShardsResults `json:"_shards,omitempty"`
	Status  int              `json:"status,omitempty"`
	Found   bool             `json:"found,omitempty"`
	Error   *ESError         `json:"error,omitempty"`
}

type ESShardsResults struct {
	Total       int `json:"total,omitempty"`
	Successfult int `json:"successful,omitempty"`
	Failed      int `json:"failed,omitempty"`
}

type ESError struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ESError               `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
}
