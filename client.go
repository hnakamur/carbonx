package carbonx

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	pbz "github.com/go-graphite/carbonzipper/carbonzipperpb3"
)

var ErrNotFound = errors.New("not found")

type InfoResponse = pbz.InfoResponse
type FetchResponse = pbz.FetchResponse

type Client struct {
	httpClient      *http.Client
	carbonserverURL *url.URL
}

type ClientOption func(c *Client) error

func NewClient(options ...ClientOption) (*Client, error) {
	c := &Client{}
	for _, o := range options {
		err := o(c)
		if err != nil {
			return nil, err
		}
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	return c, nil
}

func SetCarbonserverURL(carbonserverURL string) ClientOption {
	return func(c *Client) error {
		serverURL, err := url.Parse(carbonserverURL)
		if err != nil {
			return err
		}
		c.carbonserverURL = serverURL
		return nil
	}
}

func SetHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) error {
		c.httpClient = httpClient
		return nil
	}
}

func (c *Client) GetMetricInfo(name string) (*InfoResponse, error) {
	u := url.URL{
		Scheme:   c.carbonserverURL.Scheme,
		Host:     c.carbonserverURL.Host,
		Path:     "/info/",
		RawQuery: fmt.Sprintf("format=protobuf3&target=%s", name),
	}
	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		info := &pbz.InfoResponse{}
		err = info.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		return info, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, errors.New("unexpected status from info")
	}
}

func (c *Client) FetchData(name string, from, until time.Time) (*FetchResponse, error) {
	u := url.URL{
		Scheme:   c.carbonserverURL.Scheme,
		Host:     c.carbonserverURL.Host,
		Path:     "/render/",
		RawQuery: fmt.Sprintf("format=protobuf3&target=%s&from=%d&until=%d", name, from.Unix(), until.Unix()),
	}
	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		result := &pbz.MultiFetchResponse{}
		err = result.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		metrics := result.GetMetrics()
		if len(metrics) != 1 {
			return nil, errors.New("unexpected metrics count in MultiFetchResponse")
		}
		return &metrics[0], nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, errors.New("unexpected status from info")
	}
}

// // SendPickleData sends data to the TCP pickle receiver of a go-carbon server.
// func SendPickleData(address string, metric string, dataPoints []pickle.DataPoint, maxPointsInFrame int) error {
//     conn, err := net.Dial("tcp", address)
//     if err != nil {
//         return fmt.Errorf("failed to connect to go-carbon, err=%v", err)
//     }
//     defer conn.Close()
//
//     framedConn, err := framing.NewConn(conn, byte(4), binary.BigEndian)
//     if err != nil {
//         return fmt.Errorf("failed to create framing connection, err=%v", err)
//     }
//
//     for i := 0; i < len(dataPoints); i += maxPointsInFrame {
//         end := i + maxPointsInFrame
//         if len(dataPoints) < end {
//             end = len(dataPoints)
//         }
//
//         msg := pickle.Message{Name: metric, Points: dataPoints[i:end]}
//         data, err := pickle.MarshalMessages([]pickle.Message{msg})
//         if err != nil {
//             return fmt.Errorf("failed to marshal points, err=%v", err)
//         }
//
//         _, err = framedConn.Write(data)
//         if err != nil {
//             return fmt.Errorf("failed to write data to go-carbon, err=%v", err)
//         }
//     }
//
//     return nil
// }
