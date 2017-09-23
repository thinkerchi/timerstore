package timerstore

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"dana-tech.com/wbw/logs"
)

const (
	nilMsg       = "redis: nil"
	sortedSetKey = "timerstore"
)

// 用三个数据模型来存储相关数据
// 1. redis key=用户设置的key, value=entry的json序列化字符串, 供用户根据key快速获取value
// 2. redis key=过期时间, value=1的key数组的json序列化字符串, 存储在此时间过期的所有key
// 3. 一个Sorted set, 有序存储所有过期时间, 用于快速遍历取出过期时间集

type redisProvider struct {
	prefix string
}

// NewRedisProvider 对外提供构造redisProvider的方法
func NewRedisProvider(config *Config) (*redisProvider, error) {
	if err := InitRedis(config); err != nil {
		return nil, err
	}

	r := new(redisProvider)

	return r, nil
}

func (r *redisProvider) SetPrefix(prefix string) {
	r.prefix = prefix
}

func (r *redisProvider) Get(key string) (string, bool, error) {

	storeKey := fmt.Sprintf("%s:%s", r.prefix, key)
	data, err := DaClient.Get(storeKey).Bytes()
	if err != nil {
		if err.Error() == nilMsg {
			return "", false, nil
		}
		return "", false, err
	}

	var ent entry
	if err := json.Unmarshal(data, &ent); err != nil {
		return "", false, err
	}

	return ent.Value, true, nil
}

func (r *redisProvider) Set(key string, val string, ttl int64) error {

	due := time.Now().Unix() + ttl
	timerKey := fmt.Sprintf("%s:%d", r.prefix, due)
	storeKey := fmt.Sprintf("%s:%s", r.prefix, key)
	setKey := fmt.Sprintf("%s:%s", r.prefix, sortedSetKey)

	item := entry{
		TimerKey: timerKey,
		Value:    val,
	}

	data, _ := json.Marshal(item)

	err := DaClient.Get(storeKey).Err()
	if err != nil && err.Error() != nilMsg {
		return err
	}
	if err == nil {
		// 有旧值存在, 需要删除旧值
		if err = r.Del(key); err != nil {
			return err
		}
	}

	var storeKeys []string
	keysBytes, err := DaClient.Get(timerKey).Bytes()
	if err != nil && err.Error() != nilMsg {
		return err
	}
	if err == nil {
		if err = json.Unmarshal(keysBytes, &storeKeys); err != nil {
			return err
		}
	}

	storeKeys = append(storeKeys, storeKey)
	keysBytes, _ = json.Marshal(storeKeys)

	if err = DaClient.Set(storeKey, string(data), time.Duration(0)).Err(); err != nil {
		return err
	}

	if err = DaClient.Set(timerKey, string(keysBytes), time.Duration(0)).Err(); err != nil {
		return err
	}

	if err = DaClient.ZAdd(setKey, NewZ(due, timerKey)).Err(); err != nil {
		if len(storeKeys) == 1 {
			// 重复添加相同元素,可能会导致redis返回错误
			return err
		}
		logs.Logger.Debugf("zadd sorted set key: %s value: %s, error: %v", setKey, timerKey, err.Error())
	}

	return nil
}

func (r *redisProvider) Del(key string) error {
	storeKey := fmt.Sprintf("%s:%s", r.prefix, key)
	data, err := DaClient.Get(storeKey).Bytes()
	if err != nil && err.Error() != nilMsg {
		return err
	}
	if err == nil {
		var ent entry
		if err = json.Unmarshal(data, &ent); err != nil {
			return err
		}

		storeKeysBytes, err := DaClient.Get(ent.TimerKey).Bytes()
		if err != nil {
			return err
		}
		var storeKeys []string
		if err = json.Unmarshal(storeKeysBytes, &storeKeys); err != nil {
			return err
		}
		storeKeys = delItem(storeKeys, storeKey)

		if len(storeKeys) == 0 {
			if err = DaClient.Del(ent.TimerKey).Err(); err != nil {
				return err
			}
		} else {
			storeKeysBytes, _ = json.Marshal(storeKeys)
			if err = DaClient.Set(ent.TimerKey, string(storeKeysBytes), time.Duration(0)).Err(); err != nil {
				return err
			}
		}

		if err = DaClient.Del(storeKey).Err(); err != nil {
			return err
		}
	}

	return nil
}

func (r *redisProvider) Before(t int64) (map[string]string, bool, error) {

	due := make(map[string]string)
	timerKey := fmt.Sprintf("%s:%d", r.prefix, t)
	setKey := fmt.Sprintf("%s:%s", r.prefix, sortedSetKey)

	var removes []string
	var isOver = false
	for !isOver {

		timerKeys, err := DaClient.ZRange(setKey, 0, 10).Result()
		if err != nil {
			if err.Error() == nilMsg {
				break
			}
			return nil, false, err
		}

		if len(timerKeys) < 100 {
			isOver = true
		}

		for _, k := range timerKeys {
			if k <= timerKey {
				storeKeysBytes, err := DaClient.Get(k).Bytes()
				if err != nil && err.Error() != nilMsg {
					return nil, false, err
				}
				if err == nil {
					var storeKeys []string
					if err = json.Unmarshal(storeKeysBytes, &storeKeys); err != nil {
						return nil, false, err
					}
					for _, storeKey := range storeKeys {
						key := retrieveKey(storeKey)
						data, has, err := r.Get(key)
						if err != nil {
							return nil, false, err
						}
						if has {
							due[key] = data
						}
					}
				}

				removes = append(removes, k)
			} else {
				isOver = true
				break
			}
		}

		if len(removes) > 0 {
			if err := DaClient.ZRem(setKey, removes...).Err(); err != nil {
				return nil, false, err
			}
		}
	}

	var has = true
	if len(due) == 0 {
		has = false
	}

	return due, has, nil
}

func delItem(origin []string, del string) (trimed []string) {
	if len(origin) == 0 {
		return origin
	}

	var isFound = false
	var index = 0
	for i, item := range origin {
		if item == del {
			isFound = true
			index = i
			break
		}
	}

	trimed = origin
	if isFound {
		if index == len(origin)-1 {
			trimed = origin[:index]
		} else {
			trimed = append(origin[:index], origin[index+1:]...)
		}
	}

	return
}

func retrieveKey(after string) (origin string) {
	q := strings.Split(after, ":")
	if len(q) < 2 {
		return ""
	}
	return q[1]
}
