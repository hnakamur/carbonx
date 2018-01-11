package carbontest

import (
	"fmt"

	"github.com/alyu/configparser"
)

type AggregationsConfig []AggregationConfig

type AggregationConfig struct {
	Name              string
	Pattern           string
	XFilesFactor      float32
	AggregationMethod string
}

func (c *AggregationsConfig) WriteFile(filename string) error {
	cfg := configparser.NewConfiguration()
	for _, a := range []AggregationConfig(*c) {
		sec := cfg.NewSection(a.Name)
		sec.Add("pattern", a.Pattern)
		sec.Add("xFilesFactor", fmt.Sprintf("%g", a.XFilesFactor))
		sec.Add("aggregationMethod", a.AggregationMethod)
	}
	return configparser.Save(cfg, filename)
}
