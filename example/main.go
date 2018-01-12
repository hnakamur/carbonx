package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hnakamur/carbonx"
	"github.com/hnakamur/carbonx/sender"
	retry "github.com/rafaeljesus/retry-go"
)

func main() {
	ports, err := carbonx.GetFreePorts(2)
	if err != nil {
		log.Fatal(err)
	}
	ts := carbonx.TestServer{
		RootDir: "/tmp/my-carbon-test",
		//TcpPort:          ports[0],
		PicklePort:       ports[0],
		CarbonserverPort: ports[1],
		Schemas: []carbonx.SchemaConfig{
			{
				Name:       "default",
				Pattern:    "\\.*",
				Retentions: "1s:5s,5s:15s,15s:60s",
			},
		},
		Aggregations: []carbonx.AggregationConfig{
			{
				Name:              "default",
				Pattern:           "\\.*",
				XFilesFactor:      0.0,
				AggregationMethod: "sum",
			},
		},
	}

	err = ts.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("go-carbon satrted, TcpPort=%d, PickPort=%d, CarbonserverPort=%d",
		ts.TcpPort, ts.PicklePort, ts.CarbonserverPort)

	go func() {
		defer ts.Stop()

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []sender.Message{
			{
				Name: metricName,
				Points: []sender.DataPoint{
					{
						Timestamp: now.Unix(),
						Value:     3.14159,
					},
				},
			},
		}

		//s, err := sender.NewTCP(fmt.Sprintf("127.0.0.1:%d", ts.TcpPort))
		s, err := sender.NewPickle(fmt.Sprintf("127.0.0.1:%d", ts.PicklePort))
		if err != nil {
			log.Fatal(err)
		}
		err = s.Send(metrics)
		if err != nil {
			log.Fatal(err)
		}

		c, err := carbonx.NewClient(
			fmt.Sprintf("http://127.0.0.1:%d", ts.CarbonserverPort),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			log.Fatal(err)
		}

		var info *carbonx.InfoResponse
		attempts := 5
		sleepTime := 100 * time.Millisecond
		err = retry.Do(func() error {
			var err error
			info, err = c.GetMetricInfo(metricName)
			return err
		}, attempts, sleepTime)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("metricInfo=%+v", info)

		from := now.Add(-step)
		until := from
		data, err := c.FetchData(metricName, from, until)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("data=%+v", data)
	}()

	ts.Loop()
}
