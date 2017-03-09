package esindex

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

// Optimize the target index (or indices) to have the maximum number of
// segments. Segments < 1 will default to 1.
//
// Optimize blocks until the operation completes.
func Optimize(target string, segn int) error {
	uri := fmt.Sprintf("%s/_forcemerge?max_num_segments=%d", target, segn)
	resp, err := http.Post(uri, "text/plain", nil)
	if err != nil {
		return fmt.Errorf("error optimizing: (POST %v) error:%v", uri, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("non-2xx status code: %d uri:%v", resp.StatusCode, uri)
	}
	// This could block for a while
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("error reading optimizing: uri:%v error:%v", uri, err)
	}
	return nil
}
