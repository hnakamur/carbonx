package main

import (
	"log"

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
}
