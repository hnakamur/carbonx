package carbonx

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/carbonzipper/carbonzipperpb3"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/carbonx/testserver"
	"github.com/hnakamur/freeport"
	"github.com/hnakamur/netutil"
	"github.com/lomik/go-carbon/helper/carbonpb"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestSendTCP(t *testing.T) {
	rootDir, err := ioutil.TempDir("", "carbontest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	ts, err := startCarbonServer(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer ts.Kill()

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []*carbonpb.Metric{
			{
				Metric: metricName,
				Points: []carbonpb.Point{
					{
						Timestamp: uint32(now.Unix()),
						Value:     3.14159,
					},
				},
			},
		}

		s, err := sender.NewTCPSender(
			convertListenToConnect(ts.TcpListen),
			sender.NewTextMetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}
		err = s.ConnectSendClose(metrics)
		if err != nil {
			t.Fatal(err)
		}

		fetchAndVerifyMetrics(t, "TestSendTCP",
			convertListenToConnect(ts.CarbonserverListen), now, step, metrics)
	}()
	ts.Wait()
}

func TestSendProtobuf(t *testing.T) {
	rootDir, err := ioutil.TempDir("", "carbontest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	ts, err := startCarbonServer(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer ts.Kill()

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []*carbonpb.Metric{
			{
				Metric: metricName,
				Points: []carbonpb.Point{
					{
						Timestamp: uint32(now.Unix()),
						Value:     3.14159,
					},
				},
			},
		}

		s, err := sender.NewTCPSender(
			convertListenToConnect(ts.ProtobufListen),
			sender.NewProtobuf3MetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}
		err = s.ConnectSendClose(metrics)
		if err != nil {
			t.Fatal(err)
		}

		fetchAndVerifyMetrics(t, "TestSendProtobuf",
			convertListenToConnect(ts.CarbonserverListen), now, step, metrics)
	}()
	ts.Wait()
}

func startCarbonServer(rootDir string) (*testserver.Carbon, error) {
	ports, err := freeport.GetFreePorts(3)
	if err != nil {
		return nil, err
	}
	ts := &testserver.Carbon{
		RootDir:            rootDir,
		TcpListen:          fmt.Sprintf("127.0.0.1:%d", ports[0]),
		ProtobufListen:     fmt.Sprintf("127.0.0.1:%d", ports[1]),
		CarbonserverListen: fmt.Sprintf("127.0.0.1:%d", ports[2]),
		Schemas: []testserver.SchemaConfig{
			{
				Name:       "default",
				Pattern:    "\\.*",
				Retentions: "1s:5s,5s:15s,15s:60s",
			},
		},
		Aggregations: []testserver.AggregationConfig{
			{
				Name:              "default",
				Pattern:           "\\.*",
				XFilesFactor:      0.0,
				AggregationMethod: "sum",
			},
		},
	}

	err = ts.Start()
	if err != nil {
		return nil, err
	}

	err = testserver.WaitTCPPortConnectable(
		convertListenToConnect(ts.TcpListen), 5, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	err = testserver.WaitTCPPortConnectable(
		convertListenToConnect(ts.ProtobufListen), 5, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func convertListenToConnect(listenAddr string) string {
	host, port, err := netutil.SplitHostPort(listenAddr)
	if err != nil {
		panic(err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func fetchAndVerifyMetrics(t *testing.T, testName string, carbonserverListen string, now time.Time, step time.Duration, metrics []*carbonpb.Metric) {
	u := url.URL{Scheme: "http", Host: convertListenToConnect(carbonserverListen)}
	c, err := NewClient(
		u.String(),
		&http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	for i, m := range metrics {
		var info *carbonzipperpb3.InfoResponse
		attempts := 5
		sleepTime := 100 * time.Millisecond
		err = retry.Do(func() error {
			var err error
			info, err = c.GetMetricInfo(m.Metric)
			return err
		}, attempts, sleepTime)
		if err != nil {
			t.Fatal(err)
		}
		//log.Printf("%s metricInfo=%+v", testName, info)

		from := now.Add(-step)
		until := from
		data, err := c.FetchData(m.Metric, from, until)
		if err != nil {
			t.Fatal(err)
		}
		//log.Printf("%s data=%+v", testName, data)

		got := formatMetric(convertFetchResponseToMetric(data))
		want := formatMetric(m)
		if got != want {
			t.Errorf("%s: unexptected fetch result,\nmessageIndex=%d,\ngot =%s,\nwant=%s,\ndiff=%s",
				testName, i, got, want, diff(got, want))
		}
	}
}

func formatMetric(m *carbonpb.Metric) string {
	var b []byte
	b = append(b, "Metric{Metric:"...)
	b = append(b, m.Metric...)
	b = append(b, ", Points:"...)
	for i, p := range m.Points {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, fmt.Sprintf("{Timestamp:%d,Value:%g}", p.Timestamp, p.Value)...)
	}
	b = append(b, '}')
	return string(b)
}

func diff(text1, text2 string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(text1, text2, false)
	return dmp.DiffPrettyText(diffs)
}
