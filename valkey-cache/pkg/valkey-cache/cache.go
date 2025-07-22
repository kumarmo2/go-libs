package valkeycache

import (
	"context"
	"log"
	"strconv"
	"sync"

	valkey "github.com/valkey-io/valkey-go"
)

type ICache interface {
	Get(key string) ([]byte, error)
}

type Cache struct {
	host        string
	port        int
	connectFunc func() (valkey.Client, error)
}

type CacheConfig struct {
	Host *string
	Port *int
}

func NewCache(config *CacheConfig) ICache {
	host := "localhost"
	port := 6739

	var conf *CacheConfig
	if config != nil {
		conf = config
	} else {
		conf = &CacheConfig{}
	}

	if conf.Host != nil {
		host = *conf.Host
	}

	if conf.Port != nil {
		port = *conf.Port
	}

	connect := sync.OnceValues(func() (valkey.Client, error) {
		return connect(host, port)
	})

	return &Cache{host: host, port: port, connectFunc: connect}
}

func connect(host string, port int) (valkey.Client, error) {
	options := valkey.ClientOption{}
	options.InitAddress = []string{host + ":" + strconv.Itoa(port)}
	return valkey.NewClient(options)
}

func (c *Cache) Get(key string) ([]byte, error) {
	client, err := c.connectFunc()
	if err != nil {
		log.Println("error while connecting to valkey:", err)

		// TODO: we need to perform some kind of reconnection logic here
		c.connectFunc = func() (valkey.Client, error) {
			return connect(c.host, c.port)
		}
		return nil, err
	}

	return client.Do(context.Background(), client.B().Get().Key(key).Build()).AsBytes()
}
