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
	resp, err := http.Post(fmt.Sprintf("%s/_optimize?max_num_segments=%d", target, segn), "text/plain", nil)
	if err != nil {
		return fmt.Errorf("error optimizing: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("non-2xx status code: %d", resp.StatusCode)
	}
	// This could block for a while
	if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("error while waiting on optimize: %v", err)
	}
	return nil
}
