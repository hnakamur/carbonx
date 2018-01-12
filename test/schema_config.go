package test

import "github.com/alyu/configparser"

type SchemaConfig struct {
	Name       string
	Pattern    string
	Retentions string
}

type schemasConfig []SchemaConfig

func (c schemasConfig) writeFile(filename string) error {
	cfg := configparser.NewConfiguration()
	for _, a := range []SchemaConfig(c) {
		sec := cfg.NewSection(a.Name)
		sec.Add("pattern", a.Pattern)
		sec.Add("retentions", a.Retentions)
	}
	return configparser.Save(cfg, filename)
}
