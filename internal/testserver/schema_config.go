package testserver

import (
	"html/template"
	"io"
	"os"
)

const schemasConfigTmpl = `{{range . -}}
[{{.Name}}]
pattern = {{.Pattern}}
retentions = {{.Retentions}}
{{end -}}
`

type schemasConfig []SchemaConfig

type SchemaConfig struct {
	Name       string
	Pattern    string
	Retentions string
}

func (c schemasConfig) writeTo(w io.Writer) error {
	tmpl := template.Must(template.New("schemasConfig").Parse(schemasConfigTmpl))
	return tmpl.Execute(w, c)
}

func (c schemasConfig) writeFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	return c.writeTo(file)
}
