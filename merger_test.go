package carbonx

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/carbonzipperpb3"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/carbonx/testserver"
	"github.com/hnakamur/freeport"
	retry "github.com/rafaeljesus/retry-go"
)

func TestDiff(t *testing.T) {
	rootDir, err := ioutil.TempDir("", "carbontest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	servers, err := startTwoCarbonServers(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer killTestCarbonServers(servers)

		const metricName = "test.access-count"
		step := time.Minute
		now := time.Now().Truncate(step)

		srcMetrics := []*carbonpb.Metric{
			{
				Metric: metricName,
				Points: []carbonpb.Point{
					{
						Timestamp: uint32(now.Add(-5 * step).Unix()),
						Value:     0,
					},
					{
						Timestamp: uint32(now.Add(-4 * step).Unix()),
						Value:     1,
					},
					{
						Timestamp: uint32(now.Add(-3 * step).Unix()),
						Value:     2,
					},
					{
						Timestamp: uint32(now.Unix()),
						Value:     3,
					},
				},
			},
		}
		destMetrics := []*carbonpb.Metric{
			{
				Metric: metricName,
				Points: []carbonpb.Point{
					{
						Timestamp: uint32(now.Add(-5 * step).Unix()),
						Value:     0,
					},
					{
						Timestamp: uint32(now.Add(-3 * step).Unix()),
						Value:     11,
					},
					{
						Timestamp: uint32(now.Add(-2 * step).Unix()),
						Value:     12,
					},
					{
						Timestamp: uint32(now.Unix()),
						Value:     13,
					},
				},
			},
		}

		srcSender, err := sender.NewTCPSender(
			convertListenToConnect(servers[0].TCPListen),
			sender.NewTextMetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}
		err = srcSender.Send(srcMetrics)
		if err != nil {
			t.Fatal(err)
		}

		destSender, err := sender.NewTCPSender(
			convertListenToConnect(servers[1].TCPListen),
			sender.NewTextMetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}
		err = destSender.Send(destMetrics)
		if err != nil {
			t.Fatal(err)
		}

		srcURL := url.URL{Scheme: "http", Host: convertListenToConnect(servers[0].CarbonserverListen)}
		srcClient, err := NewClient(
			srcURL.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		destURL := url.URL{Scheme: "http", Host: convertListenToConnect(servers[1].CarbonserverListen)}
		destClient, err := NewClient(
			destURL.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}

		var info *carbonzipperpb3.InfoResponse
		attempts := 5
		sleepTime := 100 * time.Millisecond
		err = retry.Do(func() error {
			var err error
			info, err = srcClient.GetMetricInfo(metricName)
			return err
		}, attempts, sleepTime)
		if err != nil {
			t.Fatal(err)
		}
		err = retry.Do(func() error {
			var err error
			info, err = destClient.GetMetricInfo(metricName)
			return err
		}, attempts, sleepTime)
		if err != nil {
			t.Fatal(err)
		}

		merger := NewMerger(srcClient, destClient, nil)
		from := now.Add(-5*step - step)
		until := now
		points, err := merger.Diff(metricName, from, until)
		if err != nil {
			t.Fatal(err)
		}
		if len(points) == 0 {
			log.Printf("no diff")
			return
		}

		if len(points) != 2 {
			t.Errorf("unexpected len(points), got=%d, want=%d", len(points), 2)
		}
		srcPoints := points[0]
		destPoints := points[1]
		if len(srcPoints) != len(destPoints) {
			t.Errorf("unmatched len(srcPoints)=%d, len(destPoints)=%d", len(srcPoints), len(destPoints))
		}
		//for i := 0; i < len(srcPoints); i++ {
		//	log.Printf("i=%d, srcPoint=%+v, destPoint=%+v", i, srcPoints[i], destPoints[i])
		//}

		gotSrc := formatPoints(srcPoints)
		gotDest := formatPoints(destPoints)
		wantSrc := formatPoints([]carbonpb.Point{
			{
				Timestamp: uint32(now.Add(-4 * step).Unix()),
				Value:     1,
			},
			{
				Timestamp: uint32(now.Add(-3 * step).Unix()),
				Value:     2,
			},
			{
				Timestamp: uint32(now.Add(-2 * step).Unix()),
				Value:     math.NaN(),
			},
			{
				Timestamp: uint32(now.Unix()),
				Value:     3,
			},
		})
		wantDest := formatPoints([]carbonpb.Point{
			{
				Timestamp: uint32(now.Add(-4 * step).Unix()),
				Value:     math.NaN(),
			},
			{
				Timestamp: uint32(now.Add(-3 * step).Unix()),
				Value:     11,
			},
			{
				Timestamp: uint32(now.Add(-2 * step).Unix()),
				Value:     12,
			},
			{
				Timestamp: uint32(now.Unix()),
				Value:     13,
			},
		})
		if gotSrc != wantSrc {
			t.Errorf("unexpected src points, got=%s, want=%s", gotSrc, wantSrc)
		}
		if gotDest != wantDest {
			t.Errorf("unexpected dest points, got=%s, want=%s", gotDest, wantDest)
		}
	}()
	waitTestCarbonServers(servers)
}

func TestMergeMetric(t *testing.T) {
	t.Run("singleMetricCase1", testMergeMetricSingleMetricCase1)
	t.Run("singleMetricCase2", testMergeMetricSingleMetricCase2)
}

func testMergeMetricSingleMetricCase1(t *testing.T) {
	const metricName = "test.access-count"
	step := time.Minute
	now := time.Now().Truncate(step)

	setup := func(t *testing.T, srcSender, destSender *sender.TCPSender) error {
		srcMetrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: -1},
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 1},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		destMetrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 11},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 12},
				{Timestamp: uint32(now.Unix()), Value: 13},
			},
		}}

		err := srcSender.Send(srcMetrics)
		if err != nil {
			return err
		}
		err = destSender.Send(destMetrics)
		if err != nil {
			return err
		}
		return nil
	}

	merge := func(t *testing.T, merger *Merger) error {
		return merger.MergeMetric(metricName)
	}

	verify := func(t *testing.T, merger *Merger) error {
		//// wait for sent data are written
		//time.Sleep(3 * time.Second)

		from := now.Add(-6 * step).Add(-step)
		until := now
		destData, err := merger.destClient.FetchData(metricName, from, until)
		if err != nil {
			return err
		}
		gotPointsStr := formatPoints(convertFetchedDataToPoints(destData))

		wantPoints := []carbonpb.Point{
			{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: -1},
			{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
			{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 1},
			{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 11},
			{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 12},
			{Timestamp: uint32(now.Add(-step).Unix()), Value: math.NaN()},
			{Timestamp: uint32(now.Unix()), Value: 13},
		}
		wantPointsStr := formatPoints(wantPoints)
		if gotPointsStr != wantPointsStr {
			t.Errorf("unexpected points after merge, got=%s, want=%s, diff=%s",
				gotPointsStr, wantPointsStr, diff(gotPointsStr, wantPointsStr))
		}
		return nil
	}

	testMergeHelper(t, setup, merge, verify)
}

func testMergeMetricSingleMetricCase2(t *testing.T) {
	const metricName = "test.access-count"
	step := time.Minute
	now := time.Now().Truncate(step)

	setup := func(t *testing.T, srcSender, destSender *sender.TCPSender) error {
		srcMetrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-10 * step).Unix()), Value: 13},
				{Timestamp: uint32(now.Add(-9 * step).Unix()), Value: 14},
				{Timestamp: uint32(now.Add(-8 * step).Unix()), Value: 8},
				{Timestamp: uint32(now.Add(-7 * step).Unix()), Value: 9},
				{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: -1},
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
				{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 1},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 2},
				{Timestamp: uint32(now.Unix()), Value: 3},
			},
		}}
		destMetrics := []*carbonpb.Metric{{
			Metric: metricName,
			Points: []carbonpb.Point{
				{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
				{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 11},
				{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 12},
				{Timestamp: uint32(now.Unix()), Value: 13},
			},
		}}

		err := srcSender.Send(srcMetrics)
		if err != nil {
			return err
		}
		err = destSender.Send(destMetrics)
		if err != nil {
			return err
		}
		return nil
	}

	merge := func(t *testing.T, merger *Merger) error {
		return merger.MergeMetric(metricName)
	}

	verify := func(t *testing.T, merger *Merger) error {
		// wait for sent data are written
		//time.Sleep(3 * time.Second)

		from := now.Add(-20 * step).Add(-step)
		until := now.Add(-10 * step)
		destData, err := merger.destClient.FetchData(metricName, from, until)
		if err != nil {
			return err
		}
		gotPointsStr := formatPoints(convertFetchedDataToPoints(destData))

		wantPoints := []carbonpb.Point{
			//{Timestamp: uint32(now.Add(-10 * step).Unix()), Value: 13},
			//{Timestamp: uint32(now.Add(-9 * step).Unix()), Value: 14},
			{Timestamp: uint32(now.Add(-8 * step).Unix()), Value: 8},
			{Timestamp: uint32(now.Add(-7 * step).Unix()), Value: 9},
			{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: -1},
			{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
			{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 1},
			{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 11},
			{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 12},
			{Timestamp: uint32(now.Add(-step).Unix()), Value: math.NaN()},
			{Timestamp: uint32(now.Unix()), Value: 13},
		}
		wantPointsStr := formatPoints(wantPoints)
		if gotPointsStr != wantPointsStr {
			t.Errorf("unexpected points #1 after merge, got=%s, want=%s, diff=%s",
				gotPointsStr, wantPointsStr, diff(gotPointsStr, wantPointsStr))
		}

		//from := now.Add(-8 * step).Add(-step)
		//until := now
		//destData, err := merger.destClient.FetchData(metricName, from, until)
		//if err != nil {
		//	return err
		//}
		//gotPointsStr := formatPoints(convertFetchedDataToPoints(destData))

		//wantPoints := []carbonpb.Point{
		//	//{Timestamp: uint32(now.Add(-10 * step).Unix()), Value: 13},
		//	//{Timestamp: uint32(now.Add(-9 * step).Unix()), Value: 14},
		//	{Timestamp: uint32(now.Add(-8 * step).Unix()), Value: 8},
		//	{Timestamp: uint32(now.Add(-7 * step).Unix()), Value: 9},
		//	{Timestamp: uint32(now.Add(-6 * step).Unix()), Value: -1},
		//	{Timestamp: uint32(now.Add(-5 * step).Unix()), Value: 0},
		//	{Timestamp: uint32(now.Add(-4 * step).Unix()), Value: 1},
		//	{Timestamp: uint32(now.Add(-3 * step).Unix()), Value: 11},
		//	{Timestamp: uint32(now.Add(-2 * step).Unix()), Value: 12},
		//	{Timestamp: uint32(now.Add(-step).Unix()), Value: math.NaN()},
		//	{Timestamp: uint32(now.Unix()), Value: 13},
		//}
		//wantPointsStr := formatPoints(wantPoints)
		//if gotPointsStr != wantPointsStr {
		//	t.Errorf("unexpected points #2 after merge, got=%s, want=%s, diff=%s",
		//		gotPointsStr, wantPointsStr, diff(gotPointsStr, wantPointsStr))
		//}
		return nil
	}

	testMergeHelper(t, setup, merge, verify)
}

func testMergeHelper(t *testing.T,
	setup func(t *testing.T, srcSender, destSender *sender.TCPSender) error,
	merge func(t *testing.T, merger *Merger) error,
	verify func(t *testing.T, merger *Merger) error) {

	rootDir, err := ioutil.TempDir("", "carbontest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	servers, err := startTwoCarbonServers(rootDir)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer killTestCarbonServers(servers)

		srcSender, err := sender.NewTCPSender(
			convertListenToConnect(servers[0].ProtobufListen),
			sender.NewProtobuf3MetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}

		destSender, err := sender.NewTCPSender(
			convertListenToConnect(servers[1].ProtobufListen),
			sender.NewProtobuf3MetricsMarshaler())
		if err != nil {
			t.Fatal(err)
		}

		//srcSender, err := sender.NewTCPSender(
		//	convertListenToConnect(servers[0].TCPListen),
		//	sender.NewTextMetricsMarshaler())
		//if err != nil {
		//	t.Fatal(err)
		//}

		//destSender, err := sender.NewTCPSender(
		//	convertListenToConnect(servers[1].TCPListen),
		//	sender.NewTextMetricsMarshaler())
		//if err != nil {
		//	t.Fatal(err)
		//}

		err = setup(t, srcSender, destSender)
		if err != nil {
			t.Fatal(err)
		}

		srcURL := url.URL{Scheme: "http", Host: convertListenToConnect(servers[0].CarbonserverListen)}
		srcClient, err := NewClient(
			srcURL.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		destURL := url.URL{Scheme: "http", Host: convertListenToConnect(servers[1].CarbonserverListen)}
		destClient, err := NewClient(
			destURL.String(),
			&http.Client{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}

		merger := NewMerger(srcClient, destClient, destSender)
		err = merge(t, merger)
		if err != nil {
			t.Fatal(err)
		}

		err = verify(t, merger)
		if err != nil {
			t.Fatal(err)
		}
	}()
	waitTestCarbonServers(servers)
}

func startTwoCarbonServers(rootDir string) ([]*testserver.Carbon, error) {
	const serverCount = 2
	var servers [serverCount]*testserver.Carbon

	ports, err := freeport.GetFreePorts(3 * serverCount)
	if err != nil {
		return nil, err
	}

	for i := 0; i < serverCount; i++ {
		ts := &testserver.Carbon{
			RootDir:            filepath.Join(rootDir, fmt.Sprintf("server%d", i)),
			TCPListen:          fmt.Sprintf("127.0.0.1:%d", ports[3*i]),
			ProtobufListen:     fmt.Sprintf("127.0.0.1:%d", ports[3*i+1]),
			CarbonserverListen: fmt.Sprintf("127.0.0.1:%d", ports[3*i+2]),
			Schemas: []testserver.SchemaConfig{
				{
					Name:       "default",
					Pattern:    "\\.*",
					Retentions: "1m:10m,10m:30m,30m:120m",
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

		servers[i] = ts
	}

	for _, ts := range servers[:] {
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
	}

	return servers[:], nil
}

func killTestCarbonServers(servers []*testserver.Carbon) {
	for _, s := range servers {
		s.Kill()
	}
}

func waitTestCarbonServers(servers []*testserver.Carbon) {
	for _, s := range servers {
		s.Wait()
	}
}
