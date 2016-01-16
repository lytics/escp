package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s http://host1:9200/index1/_search http://host2:9200/index2\n", os.Args[0])
		flag.PrintDefaults()
	}
}
