package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	carbontest "bitbucket.org/hnakamur/go-carbon-test"
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
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT)
		for {
			<-c
			log.Printf("stopping go-carbon")
			s.GracefulStop()
			log.Printf("exiting")
			os.Exit(0)
		}
	}()

	s.Loop()
}
