package goque

import "gopkg.in/redis.v3"

type Client struct {
	client *redis.Client
}

func NewClient(client *redis.Client) *Client {
	return &Client{client: client}
}

func (c *Client) RPush(queue, message string) error {
	return c.client.RPush(queue, message).Err()
}

func (c *Client) LPop(queue string) (message string, ok bool, err error) {
	value, err := c.client.LPop(queue).Result()
	if err != nil {
		if err == redis.Nil {
			return "", false, nil
		}

		return "", false, err
	}

	return value, true, nil
}

func (c *Client) Add(set, value string) error {
	return c.client.SAdd(set, value).Err()
}

func (c *Client) Rem(set, value string) error {
	return c.client.SRem(set, value).Err()
}

func (c *Client) Members(set string) ([]string, error) {
	return c.client.SMembers(set).Result()
}

func (c *Client) Get(key string) (value string, ok bool, err error) {
	value, err = c.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", false, nil
		}
		return "", false, err
	}
	return value, true, nil
	return "", true, nil
}

func (c *Client) Set(key, value string) error {
	return c.client.Set(key, value, 0).Err()
}

func (c *Client) Del(key string) (value int64, err error) {
	return c.client.Del(key).Result()
}

func (c *Client) By(key string, value int64) error {
	return c.client.IncrBy(key, value).Err()
}

func (c *Client) HSet(hash, key, value string) error {
	return c.client.HSet(hash, key, value).Err()
}

func (c *Client) HDel(hash, key string) (value int64, err error) {
	return c.client.HDel(hash, key).Result()
}
