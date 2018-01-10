package carbontest

import (
	"html/template"
	"io"
	"os"
	"path/filepath"

	"github.com/lomik/go-carbon/carbon"
)

const (
	carbonConfigTmpl = `[common]
user = "carbon"
graph-prefix = "carbon.agents.{host}"
metric-endpoint = "local"
max-cpu = 1
metric-interval = "1m0s"

[whisper]
data-dir = "{{.DataDir}}/"
schemas-file = "{{.SchemasFile}}"
aggregation-file = "{{.AggregationFile}}"
workers = 1
max-updates-per-second = 0
sparse-create = false
flock = false
enabled = true

[cache]
max-size = 1000000
write-strategy = "max"

[udp]
listen = ":2003"
enabled = false
log-incomplete = false
buffer-size = 0

[tcp]
listen = ":2003"
enabled = false
buffer-size = 0

[pickle]
listen = ":2004"
max-message-size = 67108864
enabled = false
buffer-size = 0

[carbonlink]
listen = "127.0.0.1:7002"
enabled = false
read-timeout = "30s"

[grpc]
listen = "127.0.0.1:{{.GRPCPort}}"
enabled = true

[tags]
enabled = false
tagdb-url = "http://127.0.0.1:8000"
tagdb-chunk-size = 32
local-dir = "{{.TaggingDir}}/"
tagdb-timeout = "1s"

[carbonserver]
listen = "127.0.0.1:{{.CarbonServerPort}}"
enabled = true
query-cache-enabled = true
query-cache-size-mb = 0
find-cache-enabled = true
buckets = 10
max-globs = 100
fail-on-max-globs = false
metrics-as-counters = false
trigram-index = true
graphite-web-10-strict-mode = true
internal-stats-dir = ""
read-timeout = "1m0s"
idle-timeout = "1m0s"
write-timeout = "1m0s"
scan-frequency = "5m0s"

[dump]
enabled = false
path = "{{.DumpDir}}/"
restore-per-second = 0

[pprof]
listen = "127.0.0.1:7007"
enabled = false
`
)

type Config struct {
	RootDir          string
	GRPCPort         int
	CarbonServerPort int
	Schemas          SchemasConfig
	Aggregation      AggregationsConfig
}

func (c *Config) CarbonConfigFilename() string {
	return filepath.Join(c.RootDir, "go-carbon.conf")
}

func (c *Config) SchemasFilename() string {
	return filepath.Join(c.RootDir, "storage-schemas.conf")
}

func (c *Config) AggregationFilename() string {
	return filepath.Join(c.RootDir, "storage-aggregation.conf")
}

func (c *Config) DataDirname() string {
	return filepath.Join(c.RootDir, "data")
}

func (c *Config) TaggingDirname() string {
	return filepath.Join(c.RootDir, "tagging")
}

func (c *Config) DumpDirname() string {
	return filepath.Join(c.RootDir, "dump")
}

func (c *Config) SetupConfigFilesAndDirs() error {
	dirs := []string{
		c.DataDirname(),
		c.TaggingDirname(),
		c.DumpDirname(),
	}
	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0700)
		if err != nil {
			return err
		}
	}

	err := c.WriteCarbonConfigFile(c.CarbonConfigFilename())
	if err != nil {
		return err
	}

	err = c.Schemas.WriteFile(c.SchemasFilename())
	if err != nil {
		return err
	}

	err = c.Aggregation.WriteFile(c.AggregationFilename())
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) WriteCarbonConfigTo(w io.Writer) error {
	tmpl := template.Must(template.New("carbon").Parse(carbonConfigTmpl))
	data := struct {
		RootDir          string
		DataDir          string
		SchemasFile      string
		AggregationFile  string
		GRPCPort         int
		CarbonServerPort int
		TaggingDir       string
		DumpDir          string
	}{
		RootDir:          c.RootDir,
		DataDir:          c.DataDirname(),
		SchemasFile:      c.SchemasFilename(),
		AggregationFile:  c.AggregationFilename(),
		GRPCPort:         c.GRPCPort,
		CarbonServerPort: c.CarbonServerPort,
		TaggingDir:       c.TaggingDirname(),
		DumpDir:          c.DumpDirname(),
	}
	return tmpl.Execute(w, data)
}

func (c *Config) WriteCarbonConfigFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	return c.WriteCarbonConfigTo(file)
}

func (c *Config) StartServer() (*carbon.App, error) {
	app := carbon.New(c.CarbonConfigFilename())
	err := app.ParseConfig()
	if err != nil {
		return nil, err
	}
	err = app.Start()
	if err != nil {
		return nil, err
	}
	return app, nil
}
