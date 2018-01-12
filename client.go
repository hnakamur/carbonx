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
	serverURL  *url.URL
	httpClient *http.Client
}

func NewClient(serverURL string, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}
	return &Client{
		serverURL:  u,
		httpClient: httpClient,
	}, nil
}

func (c *Client) GetMetricInfo(name string) (*InfoResponse, error) {
	u := url.URL{
		Scheme:   c.serverURL.Scheme,
		Host:     c.serverURL.Host,
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
		Scheme:   c.serverURL.Scheme,
		Host:     c.serverURL.Host,
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
