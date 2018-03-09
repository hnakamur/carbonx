package carbonx

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/carbonzipperpb3"
	"github.com/hnakamur/carbonx/internal/testserver"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/freeport"
	"github.com/hnakamur/netutil"
	retry "github.com/rafaeljesus/retry-go"
	"github.com/sergi/go-diff/diffmatchpatch"
)

type verifyConfig struct {
	from  time.Time
	until time.Time
	want  carbonpb.Metric
}

func TestSendText(t *testing.T) {
	t.Run("AtSeconds5nPlus0Case1", testSendAtSeconds5nPlus0Case1(createTextSender))
	t.Run("AtSeconds5nPlus0Case2", testSendAtSeconds5nPlus0Case2(createTextSender))
	t.Run("AtSeconds5nPlus0Case3", testSendAtSeconds5nPlus0Case3(createTextSender))

	t.Run("AtSeconds5nPlus1Case1", testSendAtSeconds5nPlus1Case1(createTextSender))
	t.Run("AtSeconds5nPlus1Case2", testSendAtSeconds5nPlus1Case2(createTextSender))
	t.Run("AtSeconds5nPlus1Case3", testSendAtSeconds5nPlus1Case3(createTextSender))
}

func TestSendProtobuf(t *testing.T) {
	t.Run("AtSeconds5nPlus0Case1", testSendAtSeconds5nPlus0Case1(createProtobufSender))
	t.Run("AtSeconds5nPlus0Case2", testSendAtSeconds5nPlus0Case2(createProtobufSender))
	t.Run("AtSeconds5nPlus0Case3", testSendAtSeconds5nPlus0Case3(createProtobufSender))

	t.Run("AtSeconds5nPlus1Case1", testSendAtSeconds5nPlus1Case1(createProtobufSender))
	t.Run("AtSeconds5nPlus1Case2", testSendAtSeconds5nPlus1Case2(createProtobufSender))
	t.Run("AtSeconds5nPlus1Case3", testSendAtSeconds5nPlus1Case3(createProtobufSender))
}

func testSendAtSeconds5nPlus0Case1(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Sub(now))

		now = time.Now().Truncate(step)

		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 6},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-nextStep).Add(-nextStep), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 18},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			return s.Send(metrics)
		}
		verify := func(t *testing.T, client *Client) error {
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
}

func testSendAtSeconds5nPlus0Case2(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Sub(now))

		now = time.Now().Truncate(step)
		// This input metrics causes a surprising result, so should be avoided.
		// Seperate inputs such as testSendAtSeconds5nPlus0Case3
		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: 1},
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 6},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-nextStep).Add(-nextStep), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 6},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			return s.Send(metrics)
		}
		verify := func(t *testing.T, client *Client) error {
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
}

func testSendAtSeconds5nPlus0Case3(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Sub(now))

		now = time.Now().Truncate(step)
		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-2 * nextStep).Unix()), Value: 1 + 6},
			},
		}, {
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}

		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-2 * nextStep).Add(-nextStep), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 6},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			for _, m := range metrics {
				err := s.Send([]*carbonpb.Metric{m})
				if err != nil {
					return err
				}
				//time.Sleep(time.Second)
			}
			return nil
		}
		verify := func(t *testing.T, client *Client) error {
			//time.Sleep(time.Second)
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
}

func testSendAtSeconds5nPlus1Case1(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Add(step).Sub(now))

		now = time.Now().Truncate(step)

		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 6},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-step - nextStep).Add(-nextStep), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-step - nextStep).Unix()), Value: 16},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 5},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			return s.Send(metrics)
		}
		verify := func(t *testing.T, client *Client) error {
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
}

func testSendAtSeconds5nPlus1Case2(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Add(step).Sub(now))

		now = time.Now().Truncate(step)
		// This input metrics causes a surprising result, so should be avoided.
		// Seperate inputs such as testSendAtSeconds5nPlus1Case3
		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: 1},
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 6},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-step - nextStep).Add(-nextStep), until: now.Add(-step),
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-step - nextStep).Unix()), Value: 6},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 5},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			return s.Send(metrics)
		}
		verify := func(t *testing.T, client *Client) error {
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
}

func testSendAtSeconds5nPlus1Case3(createSender func(ts *testserver.Carbon) (*sender.TCPSender, error)) func(*testing.T) {
	return func(t *testing.T) {
		const metricName = "test.access-count"
		const step = time.Second
		const nextStep = 5 * time.Second

		now := time.Now()
		time.Sleep(now.Truncate(nextStep).Add(nextStep).Add(step).Sub(now))

		now = time.Now().Truncate(step)
		metrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: 1},
			},
		}, {
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 6},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
				{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}

		verifyConfigs := []verifyConfig{{
			from: now.Add(-4 * step).Add(-step), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 7},
					{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
					{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
					{Timestamp: uint32(now.Unix()), Value: 3},
				},
			},
		}, {
			from: now.Add(-step - nextStep).Add(-nextStep), until: now,
			want: carbonpb.Metric{
				Metric: metricName,
				Points: []carbonpb.Point{
					{Timestamp: uint32(now.Add(-step - nextStep).Unix()), Value: 6},
					{Timestamp: uint32(now.Add(-step).Unix()), Value: 5},
				},
			},
		}}

		setup := func(t *testing.T, s *sender.TCPSender) error {
			return s.Send(metrics)
		}
		verify := func(t *testing.T, client *Client) error {
			return fetchAndVerifyMetrics(t, client, verifyConfigs)
		}
		testWithOneServer(t, createSender, setup, verify)
	}
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
	log.Printf("rootDir=%s", rootDir)
	//defer os.RemoveAll(rootDir)

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
				Retentions: "1s:5s,5s:20s,20s:60s",
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

func fetchAndVerifyMetrics(t *testing.T, client *Client, configs []verifyConfig) error {
	for i, c := range configs {
		_, err := waitForMetricWritten(client, c.want.Metric)
		if err != nil {
			return err
		}

		data, err := client.FetchData(c.want.Metric, c.from, c.until)
		if err != nil {
			return err
		}

		got := formatMetric(convertFetchResponseToMetric(data))
		want := formatMetric(&c.want)
		if got != want {
			t.Errorf("unexptected fetch result,\nveirfyIndex=%d, metric=%s, from=%d(%s), until=%d(%s)\ngot =%s,\nwant=%s,\ndiff=%s",
				i, c.want.Metric, c.from.Unix(), formatTime(c.from), c.until.Unix(), formatTime(c.until), got, want, diff(got, want))
		}
	}
	return nil
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
		b = append(b, '(')
		b = appendTime(b, time.Unix(int64(p.Timestamp), 0))
		b = append(b, ") Value:"...)
		b = strconv.AppendFloat(b, p.Value, 'g', -1, 64)
		b = append(b, '}')
	}
	b = append(b, ']')
	return b
}

const timeFormat = "2006-01-02 15:04:05"

func formatTime(t time.Time) string {
	var b [len(timeFormat)]byte
	return string(appendTime(b[:0], t))
}

func appendTime(b []byte, t time.Time) []byte {
	return t.AppendFormat(b, timeFormat)
}

func diff(text1, text2 string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(text1, text2, false)
	return dmp.DiffPrettyText(diffs)
}
