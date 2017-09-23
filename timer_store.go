package timerstore

import (
	"time"

	"dana-tech.com/wbw/logs"
)

var providerMgr = make(map[string]Provider)

// RegisterProvider 注册provider到providerMgr
func RegisterProvider(name string, provider Provider) error {
	if _, has := providerMgr[name]; has {
		return errDuplicate
	}

	providerMgr[name] = provider
	return nil
}

// NewProvider 根据name构建相应的Provider实例
func NewProvider(name string) Provider {
	return nil
}

// Handler 业务调用时设置的回调函数
type Handler func(key string, value string)

// Set 供业务调用
// ttl 是存储有效时间, 单位为秒
func (t *TimerStore) Set(key string, value string, ttl int64) error {
	return t.store.Set(key, value, ttl)
}

// Get 返回key值对应的value, ok表示是否获取成功
func (t *TimerStore) Get(key string) (string, bool, error) {
	return t.store.Get(key)
}

// TimerStore 定义一个定时器
type TimerStore struct {
	prefix   string        // timer的key值前缀
	store    Provider      // 定时器存储
	interval time.Duration // 循环遍历的时间间隔
	h        Handler       // 定时到期时的回调函数
}

// NewTimerStore 构造一个定时器
// prefix 存储前缀
// provider 存储类型, 内存, mysql, redis
func NewTimerStore(prefix, provider string, interval time.Duration, handler Handler) (*TimerStore, error) {
	p, ok := providerMgr[provider]
	if !ok {
		return nil, errUnkownProvider
	}

	t := &TimerStore{
		prefix:   prefix,
		store:    p,
		interval: interval,
		h:        handler,
	}
	t.store.SetPrefix(prefix)

	go t.process()

	return t, nil
}

func (t *TimerStore) process() {
	time.AfterFunc(t.interval, func() {
		due, ok, err := t.store.Before(time.Now().Unix())
		if err != nil {
			logs.Logger.Errorf("%s", err.Error())
		}
		if ok {
			for key, val := range due {
				t.h(key, val)
				t.store.Del(key)
			}
		}
		t.process()
	})
}

// Provider 定义存储层的接口, 实现可以是内存, redis, mysql等
type Provider interface {
	SetPrefix(prefix string)
	Get(key string) (string, bool, error)
	Set(key string, val string, ttl int64) error
	Del(key string) error
	Before(t int64) (map[string]string, bool, error)
}
