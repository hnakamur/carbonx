package main

import (
	"fmt"
	"log"
	"time"

	carbontest "bitbucket.org/hnakamur/go-carbon-test"
	graphite "github.com/marpaia/graphite-golang"
	retry "github.com/rafaeljesus/retry-go"
)

func main() {
	ports, err := carbontest.GetFreePorts(2)
	if err != nil {
		log.Fatal(err)
	}

	s := carbontest.Server{
		RootDir:          "/tmp/my-carbon-test",
		TcpPort:          ports[0],
		CarbonserverPort: ports[1],
		Schemas: carbontest.SchemasConfig{
			{
				Name:       "carbon",
				Pattern:    "carbon\\.*",
				Retentions: "60:90d",
			},
			{
				Name:       "default",
				Pattern:    "\\.*",
				Retentions: "1s:5s,5s:15s,15s:60s",
			},
		},
		Aggregation: carbontest.AggregationsConfig{
			{
				Name:              "carbon",
				Pattern:           "carbon\\.*",
				XFilesFactor:      0.5,
				AggregationMethod: "sum",
			},
			{
				Name:              "default",
				Pattern:           "\\.*",
				XFilesFactor:      0.0,
				AggregationMethod: "sum",
			},
		},
	}

	err = s.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("go-carbon satrted, TcpPort=%d, PickPort=%d, CarbonserverPort=%d",
		s.TcpPort, s.PicklePort, s.CarbonserverPort)

	go func() {
		defer s.ForceStop()
		g, err := graphite.NewGraphite("127.0.0.1", s.TcpPort)
		if err != nil {
			log.Fatal(err)
		}

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []graphite.Metric{
			{
				Name:      metricName,
				Value:     "3.14159",
				Timestamp: now.Unix(),
			},
		}
		err = g.SendMetrics(metrics)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("sent metrics, now=%s, timestamp=%d", now, now.Unix())
		err = g.Disconnect()
		if err != nil {
			log.Fatal(err)
		}

		carbonserverURL := fmt.Sprintf("http://127.0.0.1:%d", s.CarbonserverPort)
		c, err := carbontest.NewClient(carbontest.SetCarbonserverURL(carbonserverURL))
		if err != nil {
			log.Fatal(err)
		}

		var info *carbontest.InfoResponse
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

	s.Loop()
}
