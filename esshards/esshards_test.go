package esshards

import (
	"fmt"
	"testing"
)

func TestGetting(t *testing.T) {

	info, err := Get("http://localhost:9200/_search_shards")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%#v\n", info)

}
