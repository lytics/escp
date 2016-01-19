package esdiff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"

	"github.com/lytics/escp/estypes"
)

const (
	// DiffMissing is returned if the source document is missing in the
	// destination.
	DiffMissing = "missing"

	// DiffSource is returned if the source document and destination document
	// sources differ.
	DiffSource = "source differs"
)

// ErrHTTP is returned for non-200 responses from the destination Elasticsearch
// server.
type ErrHTTP struct {
	Code int
	Body []byte
}

func (e *ErrHTTP) Error() string {
	return fmt.Sprintf("non-200 status code: %d", e.Code)
}

// Check the source document against the destination URL. Returns a string
// describing any differences or any empty string if the documents matched.
//
// Errors from Elasticsearch or JSON unmarshalling are returned untouched
// with an empty diff string.
func Check(src *estypes.Doc, dst string) (diff string, err error) {
	// Get the document from the target index
	target := fmt.Sprintf("%s/%s/%s", dst, src.Type, src.ID)
	resp, err := http.Get(target)
	if err != nil {
		return "", err
	}
	switch resp.StatusCode {
	case 200:
		// continue on
	case 404:
		// treat as diff
		return DiffMissing, nil
	default:
		// treat all other respones as errors
		buf, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return "", &ErrHTTP{resp.StatusCode, buf}
	}

	if resp.StatusCode != 200 {
	}

	newdoc := estypes.Doc{}
	if err := json.NewDecoder(resp.Body).Decode(&newdoc); err != nil {
		log.Printf("ERROR %s -- %v", target, err)
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return "", fmt.Errorf("error decoding destination document: %v", err)
	}

	if src.ID != newdoc.ID {
		return "", fmt.Errorf("metadata mismatch; document _id %s != %s", src.ID, newdoc.ID)
	}
	if src.Type != newdoc.Type {
		return "", fmt.Errorf("metadata mismatch; document type %s != %s", src.Type, newdoc.Type)
	}

	// Fast path
	if bytes.Equal(src.Source, newdoc.Source) {
		return "", nil
	}

	// Slow path
	origsrc := map[string]interface{}{}
	if err := json.Unmarshal(src.Source, &origsrc); err != nil {
		return "", fmt.Errorf("error unmarshalling source doc: %v", err)
	}
	newsrc := make(map[string]interface{}, len(origsrc))
	if err := json.Unmarshal(newdoc.Source, &newsrc); err != nil {
		return "", fmt.Errorf("error unmarshalling destination doc: %v", err)
	}

	if len(origsrc) != len(newsrc) {
		return fmt.Sprintf("%d fields in source; %d fields in target", src.ID, len(origsrc), len(newsrc)), nil
	}

	if !reflect.DeepEqual(origsrc, newsrc) {
		return DiffSource, nil
	}

	// We're good!
	return "", nil
}
