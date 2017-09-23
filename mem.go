package timerstore

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type entry struct {
	TimerKey string
	Value    string
}

type memProvider struct {
	prefix string
	timer  map[string]*list.List
	cache  map[string]entry
	mutex  sync.RWMutex
}

// NewMemProvider 对外提供的创建方法
func NewMemProvider() *memProvider {
	return &memProvider{
		timer: make(map[string]*list.List),
		cache: make(map[string]entry),
	}
}

func (m *memProvider) SetPrefix(prefix string) {
	m.prefix = prefix
}

func (m *memProvider) Get(key string) (string, bool, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	entry, ok := m.cache[key]

	if !ok {
		return "", false, nil
	}

	return entry.Value, ok, nil
}

func (m *memProvider) Set(key string, val string, ttl int64) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	item, ok := m.cache[key]
	if ok {
		// 已存在, 去除原定时器
		l, _ := m.timer[item.TimerKey]
		if l != nil {
			for e := l.Front(); e != nil; e = e.Next() {
				if e.Value.(string) == key {
					l.Remove(e)
					break
				}
			}
			if l.Len() == 0 {
				delete(m.timer, item.TimerKey)
			}
		}
	}
	timeKey := m.genTimerKey(time.Now().Unix() + ttl)
	l, _ := m.timer[timeKey]
	if l == nil {
		l = list.New()
	}
	l.PushFront(key)
	m.timer[timeKey] = l

	item = entry{
		TimerKey: timeKey,
		Value:    val,
	}
	m.cache[key] = item

	return nil
}

func (m *memProvider) Del(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	item, ok := m.cache[key]
	if ok {
		l, _ := m.timer[item.TimerKey]
		if l != nil {
			for e := l.Front(); e != nil; e = e.Next() {
				if e.Value.(string) == key {
					l.Remove(e)
					break
				}
			}
			if l.Len() == 0 {
				delete(m.timer, item.TimerKey)
			}
		}

		delete(m.cache, key)
	}

	return nil
}

func (m *memProvider) Before(t int64) (map[string]string, bool, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	due := make(map[string]string)

	timerKey := m.genTimerKey(t)

	for tKey, l := range m.timer {
		if tKey <= timerKey {
			if l != nil {
				for e := l.Front(); e != nil; e = e.Next() {
					k := e.Value.(string)
					if item, ok := m.cache[k]; ok {
						due[k] = item.Value
					}
				}
			}
		}
	}

	has := true
	if len(due) == 0 {
		has = false
	}

	return due, has, nil
}

func (m *memProvider) genTimerKey(ttl int64) string {
	return fmt.Sprintf("%s:%d", m.prefix, ttl)
}
