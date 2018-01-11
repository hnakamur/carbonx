package carbontest

import "github.com/alyu/configparser"

type SchemasConfig []SchemaConfig

type SchemaConfig struct {
	Name       string
	Pattern    string
	Retentions string
}

func (c *SchemasConfig) WriteFile(filename string) error {
	cfg := configparser.NewConfiguration()
	for _, a := range []SchemaConfig(*c) {
		sec := cfg.NewSection(a.Name)
		sec.Add("pattern", a.Pattern)
		sec.Add("retentions", a.Retentions)
	}
	return configparser.Save(cfg, filename)
}
