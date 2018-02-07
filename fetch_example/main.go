package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/hnakamur/carbonx"
)

func main() {
	address := flag.String("addr", "127.0.0.1:8080", "carbonserver address")
	query := flag.String("query", "", "metrics query")
	fromStr := flag.String("from", "", "start time")
	untilStr := flag.String("until", "", "until time")
	flag.Parse()

	from, err := time.Parse(time.RFC3339, *fromStr)
	if err != nil {
		log.Fatal(err)
	}
	until, err := time.Parse(time.RFC3339, *untilStr)
	if err != nil {
		log.Fatal(err)
	}

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

	resp, err := c.FetchData(*query, from, until)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("resp=%+v", resp)
}
