package carbonx

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/internal/testserver"
	"github.com/hnakamur/carbonx/whisper"
	"github.com/hnakamur/ltsvlog"
)

func TestBulkUpdater_Update(t *testing.T) {
	const metricName = "test.access-count"
	const step = time.Second
	const nextStep = 5 * time.Second

	testCases := []struct {
		subtestName string
		build       func() ([]*carbonpb.Metric, []verifyConfig)
	}{
		{
			subtestName: "case1",
			build: func() ([]*carbonpb.Metric, []verifyConfig) {
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

				return metrics, verifyConfigs
			},
		},
		{
			subtestName: "case2",
			build: func() ([]*carbonpb.Metric, []verifyConfig) {
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
					from: now.Add(-5 * step).Add(-step), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 18},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}, {
					from: now.Add(-2 * nextStep).Add(-nextStep), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 18},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}}

				return metrics, verifyConfigs
			},
		},
		{
			subtestName: "case3",
			build: func() ([]*carbonpb.Metric, []verifyConfig) {
				now := time.Now()
				time.Sleep(now.Truncate(nextStep).Add(nextStep).Sub(now))

				now = time.Now().Truncate(step)

				metrics := []*carbonpb.Metric{{
					Metric: metricName,
					Points: []carbonpb.Point{
						{Timestamp: uint32(now.Add(-2 * nextStep).Unix()), Value: 6},
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
					from: now.Add(-5 * step).Add(-step), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 18},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}, {
					from: now.Add(-2 * nextStep).Add(-nextStep), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-2 * nextStep).Unix()), Value: 6},
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 18},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}}

				return metrics, verifyConfigs
			},
		},
		{
			subtestName: "case4",
			build: func() ([]*carbonpb.Metric, []verifyConfig) {
				now := time.Now()
				time.Sleep(now.Truncate(nextStep).Add(nextStep).Sub(now))

				now = time.Now().Truncate(step)

				metrics := []*carbonpb.Metric{{
					Metric: metricName,
					Points: []carbonpb.Point{
						{Timestamp: uint32(now.Add(-2 * nextStep).Unix()), Value: 6},
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
							{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 4},
							{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 5},
							{Timestamp: uint32(now.Add(-step).Unix()), Value: 2},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}, {
					from: now.Add(-5 * step).Add(-step), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 7},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}, {
					from: now.Add(-2 * nextStep).Add(-nextStep), until: now,
					want: carbonpb.Metric{
						Metric: metricName,
						Points: []carbonpb.Point{
							{Timestamp: uint32(now.Add(-2 * nextStep).Unix()), Value: 6},
							{Timestamp: uint32(now.Add(-nextStep).Unix()), Value: 7},
							{Timestamp: uint32(now.Unix()), Value: 3},
						},
					},
				}}

				return metrics, verifyConfigs
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.subtestName, func(t *testing.T) {
			testBulkUpdaterHelper(t, c.build)
		})
	}
}

func testBulkUpdaterHelper(t *testing.T,
	buildTestData func() ([]*carbonpb.Metric, []verifyConfig)) {

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

		metrics, verifyConfigs := buildTestData()

		updater, err := createBulkUpdater(ts)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range metrics {
			name, points := whisper.TimeSeriesPointsFromMetric(m)
			ltsvlog.Logger.Info().String("msg", "BulkUpdater.Update").String("metric", name).
				String("points", formatTimeSeriesPoint(points)).Log()
			err := updater.Update(name, points)
			if err != nil {
				t.Fatal(err)
			}
		}

		u := url.URL{Scheme: "http", Host: convertListenToConnect(ts.CarbonserverListen)}
		client, err := NewClient(
			u.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		err = fetchAndVerifyMetrics(t, client, verifyConfigs)
		if err != nil {
			t.Fatal(err)
		}

		seen := make(map[string]struct{})
		for _, m := range metrics {
			if _, ok := seen[m.Metric]; ok {
				continue
			}
			seen[m.Metric] = struct{}{}

			err := dumpWhisper(whisper.MetricFilePath(ts.DataDirname(), m.Metric))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	ts.Wait()
}

func createBulkUpdater(ts *testserver.Carbon) (*whisper.BulkUpdater, error) {
	return whisper.NewBulkUpdater(
		ts.DataDirname(),
		ts.SchemasFilename(),
		ts.AggregationFilename(),
		&whisper.Options{FLock: true})
}

func formatTimeSeriesPoint(points []*whisper.TimeSeriesPoint) string {
	var b []byte
	b = append(b, '[')
	for i, p := range points {
		if i > 0 {
			b = append(b, ' ')
		}
		if p == nil {
			b = append(b, "nil"...)
			continue
		}

		b = append(b, "{Time:"...)
		b = strconv.AppendInt(b, int64(p.Time), 10)
		b = append(b, '(')
		b = time.Unix(int64(p.Time), 0).AppendFormat(b, "2006-01-02 15:04:05")
		b = append(b, ") Value:"...)
		b = strconv.AppendFloat(b, p.Value, 'g', -1, 64)
		b = append(b, '}')
	}
	b = append(b, ']')
	return string(b)
}
