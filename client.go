package carbonx

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/go-graphite/carbonzipper/carbonzipperpb3"
)

var ErrNotFound = errors.New("not found")
var SkipDir = errors.New("skip this directory")

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

func (c *Client) FindMetrics(pattern string) (*carbonzipperpb3.GlobResponse, error) {
	u := url.URL{
		Scheme:   c.serverURL.Scheme,
		Host:     c.serverURL.Host,
		Path:     "/metrics/find/",
		RawQuery: fmt.Sprintf("format=protobuf&query=%s", url.QueryEscape(pattern)),
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
		info := &carbonzipperpb3.GlobResponse{}
		err = info.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		return info, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, errors.New("unexpected status from /metrics/find/")
	}
}

type FindMetricFunc func(name string, isLeaf bool, err error) error

func (c *Client) FindMetricsRecursive(name string, findMetricFn FindMetricFunc) error {
	var pattern string
	if name == "" {
		pattern = "*"
	} else {
		pattern = name + ".*"
	}

	resp, err := c.FindMetrics(pattern)
	if err != nil {
		return findMetricFn(name, false, err)
	}
	for _, m := range resp.Matches {
		err = findMetricFn(m.Path, m.IsLeaf, nil)
		if err != nil {
			if err == SkipDir {
				continue
			}
			return err
		}
		if !m.IsLeaf {
			err = c.FindMetricsRecursive(m.Path, findMetricFn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) GetMetricInfo(name string) (*carbonzipperpb3.InfoResponse, error) {
	u := url.URL{
		Scheme:   c.serverURL.Scheme,
		Host:     c.serverURL.Host,
		Path:     "/info/",
		RawQuery: fmt.Sprintf("format=protobuf&target=%s", url.QueryEscape(name)),
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
		info := &carbonzipperpb3.InfoResponse{}
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

func (c *Client) FetchData(name string, from, until time.Time) (*carbonzipperpb3.FetchResponse, error) {
	u := url.URL{
		Scheme: c.serverURL.Scheme,
		Host:   c.serverURL.Host,
		Path:   "/render/",
		RawQuery: fmt.Sprintf("format=protobuf&target=%s&from=%d&until=%d",
			name, from.Unix(), until.Unix()),
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
		result := &carbonzipperpb3.MultiFetchResponse{}
		err = result.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		metrics := result.GetMetrics()
		if len(metrics) != 1 {
			return nil, fmt.Errorf("unexpected metrics count in MultiFetchResponse, len(metrics)=%d", len(metrics))
		}
		return &metrics[0], nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, errors.New("unexpected status from info")
	}
}
