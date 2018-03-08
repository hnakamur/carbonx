package testserver

import (
	"html/template"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
)

const carbonConfigTmpl = `[common]
# Run as user. Works only in daemon mode
user = "{{.User}}"
# Prefix for store all internal go-carbon graphs. Supported macroses: {host}
graph-prefix = "carbon.agents.{host}"
# Endpoint for store internal carbon metrics. Valid values: "" or "local", "tcp://host:port", "udp://host:port"
metric-endpoint = "local"
# Interval of storing internal metrics. Like CARBON_METRIC_INTERVAL
metric-interval = "1m0s"
# Increase for configuration with multi persister workers
max-cpu = {{.MaxCPU}}

[whisper]
data-dir = "{{.DataDir}}"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-schemas-conf. Required
schemas-file = "{{.SchemasFile}}"
# http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf. Optional
aggregation-file = "{{.AggregationFile}}"
# Worker threads count. Metrics sharded by "crc32(metricName) % workers"
workers = 8
# Limits the number of whisper update_many() calls per second. 0 - no limit
max-updates-per-second = 0
# Sparse file creation
sparse-create = false
enabled = true

[cache]
# Limit of in-memory stored points (not metrics)
max-size = 1000000
# Capacity of queue between receivers and cache
# Strategy to persist metrics. Values: "max","sorted","noop"
#   "max" - write metrics with most unwritten datapoints first
#   "sorted" - sort by timestamp of first unwritten datapoint.
#   "noop" - pick metrics to write in unspecified order,
#            requires least CPU and improves cache responsiveness
write-strategy = "max"

[udp]
listen = ":2003"
enabled = false
# Enable optional logging of incomplete messages (chunked by max UDP packet size)
log-incomplete = false
# Optional internal queue between receiver and cache
buffer-size = 0

[tcp]
listen = "{{.TCPListen}}"
enabled = {{if ne .TCPListen ""}}true{{else}}false{{end}}
# Optional internal queue between receiver and cache
buffer-size = 0

[pickle]
listen = ":2004"
# Limit message size for prevent memory overflow
max-message-size = 67108864
enabled = false
# Optional internal queue between receiver and cache
buffer-size = 0

{{if ne .ProtobufListen ""}}
[receiver.protobuf]
listen = "{{.ProtobufListen}}"
protocol = "protobuf"
{{end}}

[carbonlink]
listen = "127.0.0.1:7002"
enabled = false
# Close inactive connections after "read-timeout"
read-timeout = "30s"

# grpc api
# protocol: https://github.com/lomik/go-carbon/blob/master/helper/carbonpb/carbon.proto
# samples: https://github.com/lomik/go-carbon/tree/master/api/sample
[grpc]
listen = "127.0.0.1:7003"
enabled = false

[carbonserver]
# Please NOTE: carbonserver is not intended to fully replace graphite-web
# It acts as a "REMOTE_STORAGE" for graphite-web or carbonzipper/carbonapi
listen = "{{.CarbonserverListen}}"
# Carbonserver support is still experimental and may contain bugs
# Or be incompatible with github.com/grobian/carbonserver
enabled = {{if ne .CarbonserverListen ""}}true{{else}}false{{end}}
# Buckets to track response times
buckets = 10
# carbonserver-specific metrics will be sent as counters
# For compatibility with grobian/carbonserver
metrics-as-counters = false
# Read and Write timeouts for HTTP server
read-timeout = "60s"
write-timeout = "60s"
# Enable /render cache, it will cache the result for 1 minute
query-cache-enabled = false
# 0 for unlimited
query-cache-size-mb = 0
# Enable /metrics/find cache, it will cache the result for 5 minutes
find-cache-enabled = true
# Control trigram index
#  This index is used to speed-up /find requests
#  However, it will lead to increased memory consumption
#  Estimated memory consumption is approx. 500 bytes per each metric on disk
#  Another drawback is that it will recreate index every scan-frequency interval
#  All new/deleted metrics will still be searchable until index is recreated
trigram-index = true
# carbonserver keeps track of all available whisper files
# in memory. This determines how often it will check FS
# for new or deleted metrics.
scan-frequency = "5m0s"
# Maximum amount of globs in a single metric in index
# This value is used to speed-up /find requests with
# a lot of globs, but will lead to increased memory consumption
max-globs = 100
# graphite-web-10-mode
# Use Graphite-web 1.0 native structs for pickle response
# This mode will break compatibility with graphite-web 0.9.x
# If false, carbonserver won't send graphite-web 1.0 specific structs
# That might degrade performance of the cluster
# But will be compatible with both graphite-web 1.0 and 0.9.x
graphite-web-10-strict-mode = true

[dump]
# Enable dump/restore function on USR2 signal
enabled = false
# Directory for store dump data. Should be writeable for carbon
path = "/var/lib/graphite/dump/"
# Restore speed. 0 - unlimited
restore-per-second = 0

[pprof]
listen = "localhost:7007"
enabled = false

# Default logger
[[logging]]
# logger name
# available loggers:
# * "" - default logger for all messages without configured special logger
# @TODO
logger = ""
# Log output: filename, "stderr", "stdout", "none", "" (same as "stderr")
file = "{{.LogFile}}"
# Log level: "debug", "info", "warn", "error", "dpanic", "panic", and "fatal"
level = "info"
# Log format: "json", "console", "mixed"
encoding = "mixed"
# Log time format: "millis", "nanos", "epoch", "iso8601"
encoding-time = "iso8601"
# Log duration format: "seconds", "nanos", "string"
encoding-duration = "seconds"

# You can define multiply loggers:

# Copy errors to stderr for systemd
# [[logging]]
# logger = ""
# file = "stderr"
# level = "error"
# encoding = "mixed"
# encoding-time = "iso8601"
# encoding-duration = "seconds"
`

func (c *Carbon) writeCarbonConfigTo(w io.Writer) error {
	u, err := user.Current()
	if err != nil {
		return err
	}
	tmpl := template.Must(template.New("carbon").Parse(carbonConfigTmpl))
	data := struct {
		User               string
		MaxCPU             int
		DataDir            string
		SchemasFile        string
		AggregationFile    string
		TCPListen          string
		ProtobufListen     string
		CarbonserverListen string
		LogFile            string
	}{
		User:               u.Username,
		MaxCPU:             runtime.NumCPU(),
		DataDir:            c.dataDirname(),
		SchemasFile:        c.schemasFilename(),
		AggregationFile:    c.aggregationFilename(),
		TCPListen:          c.TCPListen,
		ProtobufListen:     c.ProtobufListen,
		CarbonserverListen: c.CarbonserverListen,
		LogFile:            filepath.Join(c.logDirname(), "go-carbon.log"),
	}
	return tmpl.Execute(w, data)
}

func (c *Carbon) writeCarbonConfigFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	return c.writeCarbonConfigTo(file)
}
