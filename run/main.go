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

	cfg := carbontest.Config{
		RootDir:          "/tmp/my-carbon-test",
		GRPCPort:         ports[0],
		CarbonServerPort: ports[1],
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

	err = cfg.SetupConfigFilesAndDirs()
	if err != nil {
		log.Fatal(err)
	}

	app, err := cfg.StartServer()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("go-carbon satrted, GRPCPort=%d, CarbonServerPort=%d", cfg.GRPCPort, cfg.CarbonServerPort)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT)
		for {
			<-c
			log.Printf("stopping go-carbon")
			app.DumpStop()
			log.Printf("exiting")
			os.Exit(0)
		}
	}()

	app.Loop()
}
