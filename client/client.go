package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http2"
)

type Client struct {
	host       string
	maxTimeout time.Duration
	transport  *http2.Transport
	httpClient *http.Client
}

func NewClient(host string, maxTimeout time.Duration) *Client {
	transport := &http2.Transport{
		DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr)
		},
		AllowHTTP:          true,
		DisableCompression: true,
	}
	httpClient := &http.Client{Transport: transport}
	return &Client{
		host:       host,
		maxTimeout: maxTimeout,
		transport:  transport,
		httpClient: httpClient,
	}
}

func (c *Client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

func (c *Client) makeUrl(key []byte) string {
	return fmt.Sprintf("http://%s/%s", c.host, url.PathEscape(string(key)))
}

func (c *Client) Has(ctx context.Context, key []byte) (bool, error) {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	req, err := http.NewRequest("HEAD", c.makeUrl(key), nil)
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	// TODO: Log errors.
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func (c *Client) Get(ctx context.Context, key, buf []byte) ([]byte, error) {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	req, err := http.NewRequest("GET", c.makeUrl(key), nil)
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	} else if resp.ContentLength < 0 {
		return nil, nil
	}
	length := int(resp.ContentLength)
	if cap(buf)-len(buf) < length {
		newBuf := make([]byte, len(buf)+length)
		copy(newBuf, buf)
		buf = newBuf[:len(buf)]
	}
	off := len(buf)
	buf = buf[:off+length]
	n, err := resp.Body.Read(buf[off : off+length])
	if err != nil && err != io.EOF {
		return nil, err
	} else if n != length {
		return nil, nil
	}
	return buf, nil
}

func (c *Client) Put(ctx context.Context, key, val []byte) error {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	req, err := http.NewRequest("PUT", c.makeUrl(key), bytes.NewReader(val))
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	req, err := http.NewRequest("DELETE", c.makeUrl(key), nil)
	if err != nil {
		panic(err)
	}
	req = req.WithContext(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
