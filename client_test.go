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

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/carbonzipperpb3"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/carbonx/testserver"
	"github.com/hnakamur/freeport"
	"github.com/hnakamur/netutil"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestSendTCP(t *testing.T) {
	metricName := "test.access-count"
	step := time.Second
	now := time.Now().Truncate(step)

	metrics := []*carbonpb.Metric{{
		Metric: metricName,
		Points: []carbonpb.Point{
			{Timestamp: uint32(now.Unix()), Value: 3.14159},
		},
	}}

	setup := func(t *testing.T, s *sender.TCPSender) error {
		err := s.Send(metrics)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}

	verify := func(t *testing.T, client *Client) error {
		_, err := waitForMetricWritten(client, metricName)
		if err != nil {
			t.Fatal(err)
		}

		from := now.Add(-step)
		until := from
		data, err := client.FetchData(metricName, from, until)
		if err != nil {
			t.Fatal(err)
		}

		got := formatMetric(convertFetchResponseToMetric(data))
		want := formatMetric(metrics[0])
		if got != want {
			t.Errorf("unexptected fetch result,\ngot =%s,\nwant=%s,\ndiff=%s",
				got, want, diff(got, want))
		}
		return nil
	}

	testWithOneServer(t, createTextSender, setup, verify)
}

func TestSendProtobuf(t *testing.T) {
	metricName := "test.access-count"
	step := time.Second
	now := time.Now().Truncate(step)

	metrics := []*carbonpb.Metric{{
		Metric: metricName,
		Points: []carbonpb.Point{
			{Timestamp: uint32(now.Unix()), Value: 3.14159},
		},
	}}

	setup := func(t *testing.T, s *sender.TCPSender) error {
		err := s.Send(metrics)
		if err != nil {
			t.Fatal(err)
		}
		return nil
	}

	verify := func(t *testing.T, client *Client) error {
		_, err := waitForMetricWritten(client, metricName)
		if err != nil {
			t.Fatal(err)
		}

		from := now.Add(-step)
		until := from
		data, err := client.FetchData(metricName, from, until)
		if err != nil {
			t.Fatal(err)
		}

		got := formatMetric(convertFetchResponseToMetric(data))
		want := formatMetric(metrics[0])
		if got != want {
			t.Errorf("unexptected fetch result,\ngot =%s,\nwant=%s,\ndiff=%s",
				got, want, diff(got, want))
		}
		return nil
	}

	testWithOneServer(t, createProtobufSender, setup, verify)
}

func waitForMetricWritten(c *Client, metricName string) (*carbonzipperpb3.InfoResponse, error) {
	var info *carbonzipperpb3.InfoResponse
	attempts := 5
	sleepTime := 100 * time.Millisecond
	err := retry.Do(func() error {
		var err error
		info, err = c.GetMetricInfo(metricName)
		return err
	}, attempts, sleepTime)
	return info, err
}

func createTextSender(ts *testserver.Carbon) (*sender.TCPSender, error) {
	return sender.NewTCPSender(
		convertListenToConnect(ts.TCPListen),
		sender.NewTextMetricsMarshaler())
}

func createProtobufSender(ts *testserver.Carbon) (*sender.TCPSender, error) {
	return sender.NewTCPSender(
		convertListenToConnect(ts.ProtobufListen),
		sender.NewProtobuf3MetricsMarshaler())
}

func testWithOneServer(t *testing.T,
	createSender func(ts *testserver.Carbon) (*sender.TCPSender, error),
	setup func(t *testing.T, s *sender.TCPSender) error,
	verify func(t *testing.T, client *Client) error) {

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

		s, err := createSender(ts)
		if err != nil {
			t.Fatal(err)
		}

		err = setup(t, s)
		if err != nil {
			t.Fatal(err)
		}

		u := url.URL{Scheme: "http", Host: convertListenToConnect(ts.CarbonserverListen)}
		client, err := NewClient(
			u.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}

		err = verify(t, client)
		if err != nil {
			t.Fatal(err)
		}
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
		TCPListen:          fmt.Sprintf("127.0.0.1:%d", ports[0]),
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
		convertListenToConnect(ts.TCPListen), 5, 100*time.Millisecond)
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
	var b [256]byte
	return string(appendMetric(b[:0], m))
}

func appendMetric(b []byte, m *carbonpb.Metric) []byte {
	b = append(b, "Metric{Metric:"...)
	b = append(b, m.Metric...)
	b = append(b, " Points:"...)
	b = appendPoints(b, m.Points)
	b = append(b, '}')
	return b
}

func formatPoints(points []carbonpb.Point) string {
	var b [256]byte
	return string(appendPoints(b[:0], points))
}

func appendPoints(b []byte, points []carbonpb.Point) []byte {
	b = append(b, '[')
	for i, p := range points {
		if i > 0 {
			b = append(b, ' ')
		}
		b = append(b, "{Timestamp:"...)
		b = strconv.AppendInt(b, int64(p.Timestamp), 10)
		b = append(b, ' ')
		b = time.Unix(int64(p.Timestamp), 0).AppendFormat(b, time.RFC3339)
		b = append(b, " Value:"...)
		b = strconv.AppendFloat(b, p.Value, 'g', -1, 64)
		b = append(b, '}')
	}
	b = append(b, ']')
	return b
}

func diff(text1, text2 string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(text1, text2, false)
	return dmp.DiffPrettyText(diffs)
}
