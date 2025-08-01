package valkeycache

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	valkey "github.com/valkey-io/valkey-go"
)

type ICache interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Expire(key string) error
	UseCache() bool
}

type Cache struct {
	host        string
	port        int
	connectFunc func() (valkey.Client, error)
	config      *CacheConfig
}

type CacheConfig struct {
	Host     string
	Port     int
	UseCache bool
}

type Serializer func(T any) ([]byte, error)
type Deserializer func(bytes []byte, T any) error

var JsonSerializer Serializer = json.Marshal
var JsonDeserializer Deserializer = json.Unmarshal

func NewCache(config *CacheConfig) ICache {
	host := "localhost"
	port := 6739

	var conf *CacheConfig
	if config != nil {
		conf = config
	} else {
		conf = &CacheConfig{}
	}

	if conf.Host != "" {
		host = conf.Host
	}

	if conf.Port != 0 {
		port = conf.Port
	}

	connect := sync.OnceValues(func() (valkey.Client, error) {
		return connect(host, port)
	})

	return &Cache{host: host, port: port, connectFunc: connect, config: config}
}

func (c *Cache) UseCache() bool {
	return c.config.UseCache
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

func (c *Cache) Set(key string, value []byte) error {
	client, err := c.connectFunc()
	if err != nil {
		return err
	}
	res := client.Do(context.Background(), client.B().Set().Key(key).Value(string(value)).Build())
	res.Error()
	if res.Error() != nil {
		return res.Error()
	}
	return nil

}

func SetAndGet[T any](cache ICache, key string, getter func() (T, error)) (T, error) {
	unmarshelledValue, err := getter()
	if err != nil {
		log.Println("error while getting value from getter:", err)
		var zero T
		return zero, err
	}
	val, err := JsonSerializer(unmarshelledValue)
	if err != nil {
		log.Println("error while serializing value:", err)
		var zero T
		return zero, err
	}
	err = cache.Set(key, val)
	if err != nil {
		log.Println("error while setting value:", err)
		var zero T
		return zero, err
	}
	return unmarshelledValue, err
}

func (c *Cache) Expire(key string) error {
	cache, err := c.connectFunc()
	if err != nil {
		return err
	}
	return cache.Do(context.Background(), cache.B().Expire().Key(key).Seconds(0).Build()).Error()
}

func GetOrSetAndGet[T any](cache ICache, key string, getter func() (T, error)) (T, error) {
	if !cache.UseCache() {
		log.Println("cache is not enabled, getting from getter, key: ", key)
		return getter()
	}

	val, err := cache.Get(key)
	var unmarshelledValue T

	if err == valkey.Nil {
		log.Println("value not found in cache, getting from getter for key: ", key)
		unmarshelledValue, err = SetAndGet(cache, key, getter)
		if err != nil {
			var zero T
			return zero, err
		}
		return unmarshelledValue, nil
	} else if err != nil {
		log.Println("error while getting value from cache:", err)
		var zero T
		return zero, err
	} else {
		err = JsonDeserializer(val, &unmarshelledValue)
		if err != nil {
			log.Println("error while deserializing value:", err)
			var zero T
			return zero, err
		}
		return unmarshelledValue, nil
	}
}
