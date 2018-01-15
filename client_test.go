package carbonx_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/hnakamur/carbonx"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/carbonx/testserver"
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
		defer ts.Cmd.Process.Kill()

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []sender.Message{
			{
				Name: metricName,
				Points: []sender.DataPoint{
					{
						Timestamp: now.Unix(),
						Value:     3.14159,
					},
				},
			},
		}

		s, err := sender.NewTCP(fmt.Sprintf("127.0.0.1:%d", ts.TcpPort))
		if err != nil {
			t.Fatal(err)
		}
		err = s.Send(metrics)
		if err != nil {
			t.Fatal(err)
		}

		fetchAndVerifyMetrics(t, "TestSendTCP", ts.CarbonserverPort, now, step, metrics)
	}()
	ts.Cmd.Wait()
}

func TestSendPickle(t *testing.T) {
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
		defer ts.Cmd.Process.Kill()

		metricName := "test.access-count"
		step := time.Second
		now := time.Now().Truncate(step)
		metrics := []sender.Message{
			{
				Name: metricName,
				Points: []sender.DataPoint{
					{
						Timestamp: now.Unix(),
						Value:     3.14159,
					},
				},
			},
		}

		s, err := sender.NewPickle(fmt.Sprintf("127.0.0.1:%d", ts.PicklePort))
		if err != nil {
			t.Fatal(err)
		}
		err = s.Send(metrics)
		if err != nil {
			t.Fatal(err)
		}

		fetchAndVerifyMetrics(t, "TestSendPickle", ts.CarbonserverPort, now, step, metrics)
	}()
	ts.Cmd.Wait()
}

func startCarbonServer(rootDir string) (*testserver.Carbon, error) {
	ports, err := testserver.GetFreePorts(3)
	if err != nil {
		return nil, err
	}
	ts := &testserver.Carbon{
		RootDir:          rootDir,
		TcpPort:          ports[0],
		PicklePort:       ports[1],
		CarbonserverPort: ports[2],
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

	err = ts.Setup()
	if err != nil {
		return nil, err
	}
	cmd, err := startServer("go-carbon", "-config", ts.CarbonConfigFilename())
	if err != nil {
		return nil, err
	}
	ts.Cmd = cmd

	err = testserver.WaitPortConnectable(fmt.Sprintf("127.0.0.1:%d", ts.TcpPort), 5, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	err = testserver.WaitPortConnectable(fmt.Sprintf("127.0.0.1:%d", ts.PicklePort), 5, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func startServer(execFilename string, arg ...string) (*exec.Cmd, error) {
	path, err := exec.LookPath(execFilename)
	if err != nil {
		return nil, fmt.Errorf("executable %q not found in $PATH", execFilename)
	}
	cmd := exec.Command(path, arg...)
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func fetchAndVerifyMetrics(t *testing.T, testName string, carbonserverPort int, now time.Time, step time.Duration, messages []sender.Message) {
	c, err := carbonx.NewClient(
		fmt.Sprintf("http://127.0.0.1:%d", carbonserverPort),
		&http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	for i, message := range messages {
		var info *carbonx.InfoResponse
		attempts := 5
		sleepTime := 100 * time.Millisecond
		err = retry.Do(func() error {
			var err error
			info, err = c.GetMetricInfo(message.Name)
			return err
		}, attempts, sleepTime)
		if err != nil {
			t.Fatal(err)
		}
		// log.Printf("%s metricInfo=%+v", testName, info)

		from := now.Add(-step)
		until := from
		data, err := c.FetchData(message.Name, from, until)
		if err != nil {
			t.Fatal(err)
		}
		// log.Printf("%s data=%+v", testName, data)

		got := formatMessage(convertFetchResponseToMessage(data))
		want := formatMessage(&message)
		if got != want {
			t.Errorf("%s: unexptected fetch result,\nmessageIndex=%d,\ngot =%s,\nwant=%s,\ndiff=%s",
				testName, i, got, want, diff(got, want))
		}
	}
}

func convertFetchResponseToMessage(resp *carbonx.FetchResponse) *sender.Message {
	msg := &sender.Message{
		Name: resp.Name,
	}
	for i, v := range resp.Values {
		if resp.IsAbsent[i] {
			continue
		}
		msg.Points = append(msg.Points, sender.DataPoint{
			Timestamp: int64(resp.StartTime) + int64(i)*int64(resp.StepTime),
			Value:     v,
		})
	}
	return msg
}

func formatMessage(m *sender.Message) string {
	var b []byte
	b = append(b, "Message{Name:"...)
	b = append(b, m.Name...)
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
