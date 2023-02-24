package client

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Client struct {
	host       string
	maxTimeout time.Duration

	client *redis.Client
}

func NewClient(addr string, maxTimeout time.Duration) *Client {
	opts := &redis.Options{
		Addr:         addr,
		MaxRetries:   -1,
		DialTimeout:  maxTimeout,
		ReadTimeout:  maxTimeout,
		WriteTimeout: maxTimeout,
	}
	client := redis.NewClient(opts)
	return &Client{
		host:       addr,
		maxTimeout: maxTimeout,
		client:     client,
	}
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	// TODO: Implement
	return nil
}

func (c *Client) Has(ctx context.Context, key []byte) (bool, error) {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	count, err := c.client.Exists(ctx, string(key)).Result()
	if err != nil {
		return false, err
	}
	return count == 1, nil
}

func (c *Client) Get(ctx context.Context, key, buf []byte) ([]byte, error) {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	val, err := c.client.Get(ctx, string(key)).Result()
	if err != nil {
		return nil, err
	}
	return append(buf, val...), nil
}

func (c *Client) Put(ctx context.Context, key, val []byte) error {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	status, err := c.client.Set(ctx, string(key), string(val), 0).Result()
	if err != nil {
		return err
	} else if status != "OK" {
		return fmt.Errorf("redis error: %s", status)
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	var cf context.CancelFunc
	if c.maxTimeout > 0 {
		ctx, cf = context.WithTimeout(ctx, c.maxTimeout)
		defer cf()
	}

	_, err := c.client.Del(ctx, string(key)).Result()
	return err
}
