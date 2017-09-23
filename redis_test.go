package timerstore

import (
	"fmt"
	"testing"
	"time"
)

func TestRedisProvider(t *testing.T) {

	equal := func(expected, got interface{}) {
		if got != expected {
			t.Fatalf("expeted: %v, got: %v", expected, got)
		}
	}

	config := &Config{
		Host:        "",
		Port:        "",
		Password:    "",
		Type:        "cluster",
		PoolSize:    10,
		PoolTimeout: 10,
	}

	r, err := NewRedisProvider(config)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	r.SetPrefix("Test")

	err = r.Set("cycy", string("this is a test"), 4)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	err = r.Set("cycy1", string("this is a test1"), 4)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	time.Sleep(1 * time.Second)

	var valStr string
	valStr, ok, err := r.Get("cycy")
	fmt.Printf("ok: %v, err: %v, val: %v\n", ok, err, string(valStr))
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	equal(true, ok)
	equal("this is a test", string(valStr))

	due, has, err := r.Before(time.Now().Unix())
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	fmt.Printf("has: %v, due: %v\n", has, due)

	fmt.Printf("===>> sleep starting\n")
	time.Sleep(time.Duration(5) * time.Second)

	fmt.Printf("===>> sleep over\n")

	due, has, err = r.Before(time.Now().Unix())
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	fmt.Printf("has: %v, due: %v\n", has, due)
}

func registerRedisProvider(t *testing.T) Provider {
	config := &Config{
		Host:        "",
		Port:        "",
		Password:    "",
		Type:        "cluster",
		PoolSize:    10,
		PoolTimeout: 10,
	}

	r, err := NewRedisProvider(config)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	RegisterProvider("redis", r)
	if providerMgr["redis"] != r {
		t.Fatalf("register error")
	}

	return r
}

func TestRedisTimerStore(t *testing.T) {

	registerRedisProvider(t)

	store, err := NewTimerStore("Test", "redis", 1*time.Second, func(key string, val string) {
		fmt.Printf("Call back--->> now: %v,  key: %s, val: %v\n", time.Now().Unix(), key, val)
	})

	fmt.Printf("now: %v\n", time.Now().Unix())

	err = store.Set("first", "this is the first", 4)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	err = store.Set("second", "this is the second", 4)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	err = store.Set("third", "this is the third", 5)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	strVal, ok, err := store.Get("first")
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	if ok == false {
		t.Fatalf("expected: %v, got: %v", true, ok)
	}
	if strVal != "this is the first" {
		t.Fatalf("expected: %v, got: %v", "this is the first", strVal)
	}

	strVal, ok, err = store.Get("second")
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	if ok == false {
		t.Fatalf("expected: %v, got: %v", true, ok)
	}
	if strVal != "this is the second" {
		t.Fatalf("expected: %v, got: %v", "this is the second", strVal)
	}

	time.Sleep(4 * time.Second)

	strVal, ok, err = store.Get("first")
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	if ok == true {
		// t.Fatalf("expected: %v, got: %v", false, ok)
	}

	fmt.Printf("now: %v\n", time.Now().Unix())

	store.Set("fourth", "this is the fourth", 2)
	err = store.Set("fourth", "this is the fourth placement", 3)
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}

	strVal, ok, err = store.Get("fourth")
	if err != nil {
		t.Fatalf("expected: %v, got: %v", nil, err)
	}
	if ok == false {
		t.Fatalf("expected: %v, got: %v", true, ok)
	}
	if strVal != "this is the fourth placement" {
		t.Fatalf("expected: %v, got: %v", "this is the fourth placement", strVal)
	}

	time.Sleep(7 * time.Second)
}

func BenchmarkRedisStore10K(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkRedisStore(10000, b)
	}
}

func BenchmarkRedisStore100K(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkRedisStore(100000, b)
	}
}

func BenchmarkRedisStore1M(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchmarkRedisStore(1000000, b)
	}
}

func benchmarkRedisStore(n int, b *testing.B) {
	config := &Config{
		Host:        "",
		Port:        "",
		Password:    "",
		Type:        "cluster",
		PoolSize:    10,
		PoolTimeout: 10,
	}

	r, err := NewRedisProvider(config)
	if err != nil {
		b.Fatalf("expected: %v, got: %v", nil, err)
	}

	RegisterProvider("redis", r)

	store, err := NewTimerStore("Test", "redis", 1*time.Second, func(key, value string) {})
	if err != nil {
		b.Fatalf("expected: %v, got: %v", nil, err)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("hela_%d", i)
		val := fmt.Sprintf("value_%d", i)
		ttl := int64(i)
		store.Set(key, val, ttl)
		store.Get(key)
	}
}

func TestBefore(t *testing.T) {
	r := registerRedisProvider(t)
	r.SetPrefix("Test")

	st := time.Now()

	due, has, err := r.Before(time.Now().Unix())
	if err != nil {
		t.Fatalf("%v", err.Error())
	}
	if has == false {
		t.Fatalf("%v", has)
	}

	cost := time.Since(st)

	fmt.Printf("cost time: %v, len: %d, due: %v\n", cost.String(), len(due), due)

}
