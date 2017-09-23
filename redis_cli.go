package timerstore

import (
	"fmt"
	"time"

	"gopkg.in/redis.v3"
)

// Config is needed when initializing redis client
type Config struct {
	Host        string
	Port        string
	Password    string
	Type        string
	PoolSize    int
	PoolTimeout int
}

// DClient 是redis 集群或单机模式的客户端的抽象接口
type DClient interface {
	Set(string, interface{}, time.Duration) *redis.StatusCmd
	Get(string) *redis.StringCmd
	HGetAllMap(string) *redis.StringStringMapCmd
	HMSetMap(string, map[string]string) *redis.StatusCmd
	LPush(string, ...string) *redis.IntCmd
	LTrim(string, int64, int64) *redis.StatusCmd
	Del(keys ...string) *redis.IntCmd
	ZAdd(key string, members ...redis.Z) *redis.IntCmd
	ZRange(key string, start, stop int64) *redis.StringSliceCmd
	ZRem(key string, members ...string) *redis.IntCmd
}

// DaClient 全局共用redis client
var DaClient DClient

// NewRedisClient initializes a redis client
func NewRedisClient(config *Config) (client DClient, err error) {
	if config.Type == "cluster" {
		options := &redis.ClusterOptions{
			Addrs:       []string{config.Host + ":" + config.Port},
			Password:    config.Password,
			PoolSize:    config.PoolSize,
			PoolTimeout: time.Duration(config.PoolTimeout) * time.Second,
		}
		client = redis.NewClusterClient(options)
	} else if config.Type == "client" {
		options := &redis.Options{
			Addr:        config.Host + ":" + config.Port,
			Password:    config.Password,
			PoolSize:    config.PoolSize,
			PoolTimeout: time.Duration(config.PoolTimeout) * time.Second,
		}
		client = redis.NewClient(options)
	} else {
		err = fmt.Errorf("Redis init failed, redistype not define")
		return
	}

	return
}

// InitRedis 初始化DaClient
func InitRedis(config *Config) (err error) {
	DaClient, err = NewRedisClient(config)
	return
}

// NewZ 生成redis.Z对象
func NewZ(score int64, member interface{}) redis.Z {
	return redis.Z{
		Score:  float64(score),
		Member: member,
	}
}
