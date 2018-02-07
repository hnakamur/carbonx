package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hnakamur/carbonx"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "carbonserver address")
	name := flag.String("name", "", "metrics query pattern prefix")
	flag.Parse()

	u := url.URL{
		Scheme: "http",
		Host:   *address,
	}
	c, err := carbonx.NewClient(
		u.String(),
		&http.Client{Timeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}

	err = c.FindMetricsRecursive(*name, func(name string, isLeaf bool, err error) error {
		if err != nil {
			return err
		}
		log.Printf("name=%s, isLeaf=%v", name, isLeaf)
		if !strings.HasPrefix(name, "carbon") {
			return carbonx.SkipDir
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
