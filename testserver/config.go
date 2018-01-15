package testserver

import (
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-carbon/receiver/tcp"
	"github.com/lomik/go-carbon/receiver/udp"
	"github.com/lomik/zapwriter"
)

const MetricEndpointLocal = "local"

// Duration wrapper time.Duration for TOML
type Duration struct {
	time.Duration
}

var _ toml.TextMarshaler = &Duration{}

// UnmarshalText from TOML
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText encode text with TOML format
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// Value return time.Duration value
func (d *Duration) Value() time.Duration {
	return d.Duration
}

type commonConfig struct {
	User           string    `toml:"user"`
	Logfile        *string   `toml:"logfile"`
	LogLevel       *string   `toml:"log-level"`
	GraphPrefix    string    `toml:"graph-prefix"`
	MetricInterval *Duration `toml:"metric-interval"`
	MetricEndpoint string    `toml:"metric-endpoint"`
	MaxCPU         int       `toml:"max-cpu"`
}

type whisperConfig struct {
	DataDir             string `toml:"data-dir"`
	SchemasFilename     string `toml:"schemas-file"`
	AggregationFilename string `toml:"aggregation-file"`
	Workers             int    `toml:"workers"`
	MaxUpdatesPerSecond int    `toml:"max-updates-per-second"`
	Sparse              bool   `toml:"sparse-create"`
	FLock               bool   `toml:"flock"`
	Enabled             bool   `toml:"enabled"`
	Schemas             persister.WhisperSchemas
	Aggregation         *persister.WhisperAggregation
}

type cacheConfig struct {
	MaxSize       uint32 `toml:"max-size"`
	WriteStrategy string `toml:"write-strategy"`
}

type carbonlinkConfig struct {
	Listen      string    `toml:"listen"`
	Enabled     bool      `toml:"enabled"`
	ReadTimeout *Duration `toml:"read-timeout"`
}

type grpcConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type receiverConfig struct {
	DSN string `toml:"dsn"`
}

type tagsConfig struct {
	Enabled        bool      `toml:"enabled"`
	TagDB          string    `toml:"tagdb-url"`
	TagDBTimeout   *Duration `toml:"tagdb-timeout"`
	TagDBChunkSize int       `toml:"tagdb-chunk-size"`
	LocalDir       string    `toml:"local-dir"`
}

type carbonserverConfig struct {
	Listen                  string    `toml:"listen"`
	Enabled                 bool      `toml:"enabled"`
	ReadTimeout             *Duration `toml:"read-timeout"`
	IdleTimeout             *Duration `toml:"idle-timeout"`
	WriteTimeout            *Duration `toml:"write-timeout"`
	ScanFrequency           *Duration `toml:"scan-frequency"`
	QueryCacheEnabled       bool      `toml:"query-cache-enabled"`
	QueryCacheSizeMB        int       `toml:"query-cache-size-mb"`
	FindCacheEnabled        bool      `toml:"find-cache-enabled"`
	Buckets                 int       `toml:"buckets"`
	MaxGlobs                int       `toml:"max-globs"`
	FailOnMaxGlobs          bool      `toml:"fail-on-max-globs"`
	MetricsAsCounters       bool      `toml:"metrics-as-counters"`
	TrigramIndex            bool      `toml:"trigram-index"`
	GraphiteWeb10StrictMode bool      `toml:"graphite-web-10-strict-mode"`
	InternalStatsDir        string    `toml:"internal-stats-dir"`
	Percentiles             []int     `toml:"stats-percentiles"`
}

type pprofConfig struct {
	Listen  string `toml:"listen"`
	Enabled bool   `toml:"enabled"`
}

type dumpConfig struct {
	Enabled          bool   `toml:"enabled"`
	Path             string `toml:"path"`
	RestorePerSecond int    `toml:"restore-per-second"`
}

// Config ...
// NOTE: We had to copy Config from github.com/lomik/go-carbon to workaround the folloing error.
// cannot use []"github.com/lomik/zapwriter".Config literal (type []"github.com/lomik/zapwriter".Config) as type []"github.com/lomik/go-carbon/vendor/github.com/lomik/zapwriter".Config in assignment
type Config struct {
	Common       commonConfig                        `toml:"common"`
	Whisper      whisperConfig                       `toml:"whisper"`
	Cache        cacheConfig                         `toml:"cache"`
	Udp          *udp.Options                        `toml:"udp"`
	Tcp          *tcp.Options                        `toml:"tcp"`
	Pickle       *tcp.FramingOptions                 `toml:"pickle"`
	Receiver     map[string](map[string]interface{}) `toml:"receiver"`
	Carbonlink   carbonlinkConfig                    `toml:"carbonlink"`
	Grpc         grpcConfig                          `toml:"grpc"`
	Tags         tagsConfig                          `toml:"tags"`
	Carbonserver carbonserverConfig                  `toml:"carbonserver"`
	Dump         dumpConfig                          `toml:"dump"`
	Pprof        pprofConfig                         `toml:"pprof"`
	Logging      []zapwriter.Config                  `toml:"logging"`
}

func NewLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/go-carbon/go-carbon.log"
	return cfg
}

// NewConfig ...
func NewConfig() *Config {
	cfg := &Config{
		Common: commonConfig{
			GraphPrefix: "carbon.agents.{host}",
			MetricInterval: &Duration{
				Duration: time.Minute,
			},
			MetricEndpoint: MetricEndpointLocal,
			MaxCPU:         1,
			User:           "carbon",
		},
		Whisper: whisperConfig{
			DataDir:             "/var/lib/graphite/whisper/",
			SchemasFilename:     "/etc/go-carbon/storage-schemas.conf",
			AggregationFilename: "",
			MaxUpdatesPerSecond: 0,
			Enabled:             true,
			Workers:             1,
			Sparse:              false,
			FLock:               false,
		},
		Cache: cacheConfig{
			MaxSize:       1000000,
			WriteStrategy: "max",
		},
		Udp:    udp.NewOptions(),
		Tcp:    tcp.NewOptions(),
		Pickle: tcp.NewFramingOptions(),
		Carbonserver: carbonserverConfig{
			Listen:            "127.0.0.1:8080",
			Enabled:           false,
			Buckets:           10,
			MaxGlobs:          100,
			FailOnMaxGlobs:    false,
			MetricsAsCounters: false,
			ScanFrequency: &Duration{
				Duration: 300 * time.Second,
			},
			ReadTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			IdleTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			WriteTimeout: &Duration{
				Duration: 60 * time.Second,
			},
			QueryCacheEnabled:       true,
			QueryCacheSizeMB:        0,
			FindCacheEnabled:        true,
			TrigramIndex:            true,
			GraphiteWeb10StrictMode: true,
		},
		Carbonlink: carbonlinkConfig{
			Listen:  "127.0.0.1:7002",
			Enabled: true,
			ReadTimeout: &Duration{
				Duration: 30 * time.Second,
			},
		},
		Grpc: grpcConfig{
			Listen:  "127.0.0.1:7003",
			Enabled: true,
		},
		Tags: tagsConfig{
			Enabled: false,
			TagDB:   "http://127.0.0.1:8000",
			TagDBTimeout: &Duration{
				Duration: time.Second,
			},
			TagDBChunkSize: 32,
			LocalDir:       "/var/lib/graphite/tagging/",
		},
		Pprof: pprofConfig{
			Listen:  "127.0.0.1:7007",
			Enabled: false,
		},
		Dump: dumpConfig{
			Path: "/var/lib/graphite/dump/",
		},
		Logging: nil,
	}

	return cfg
}
