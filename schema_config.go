package carbontest

import (
	"html/template"
	"io"
	"os"
)

const (
	schemasConfigTmpl = `{{range . -}}
[{{.Name}}]
pattern = {{.Pattern}}
retentions = {{.Retentions}}
{{end -}}
`
)

type SchemasConfig []SchemaConfig

type SchemaConfig struct {
	Name       string
	Pattern    string
	Retentions string
}

func (c *SchemasConfig) WriteTo(w io.Writer) error {
	tmpl := template.Must(template.New("schemasConfig").Parse(schemasConfigTmpl))
	return tmpl.Execute(w, c)
}

func (c *SchemasConfig) WriteFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	return c.WriteTo(file)
}
