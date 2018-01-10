package carbontest

import (
	"html/template"
	"io"
	"os"
)

const (
	aggregationConfigTmpl = `{{range . -}}
[{{.Name}}]
pattern = {{.Pattern}}
xFilesFactor = {{.XFilesFactor}}
aggregationMethod = {{.AggregationMethod}}
{{end -}}
`
)

type AggregationsConfig []AggregationConfig

type AggregationConfig struct {
	Name              string
	Pattern           string
	XFilesFactor      float32
	AggregationMethod string
}

func (c *AggregationsConfig) WriteTo(w io.Writer) error {
	tmpl := template.Must(template.New("aggregationConfig").Parse(aggregationConfigTmpl))
	return tmpl.Execute(w, c)
}

func (c *AggregationsConfig) WriteFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	return c.WriteTo(file)
}
