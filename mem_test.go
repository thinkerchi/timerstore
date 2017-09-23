package timerstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMemProvider(t *testing.T) {

	equal := func(expected, got interface{}) {
		if got != expected {
			t.Fatalf("expeted: %v, got: %v", expected, got)
		}
	}

	mem := NewMemProvider()
	mem.SetPrefix("Test")
	equal("Test", mem.prefix)

	mem.Set("first", ("this is the first"), 5)
	data, ok, err := mem.Get("first")
	equal(nil, err)
	equal(true, ok)
	equal("this is the first", string(data))

	err = mem.Set("first", ("this is the first replacement"), 4)
	equal(nil, err)
	data, _, _ = mem.Get("first")
	equal("this is the first replacement", string(data))

	mem.Set("second", ("this is the second"), 10)

	due, ok, _ := mem.Before(time.Now().Unix() + 15)
	equal(true, ok)
	equal(2, len(due))
	t.Logf("due: %v\n", due)

	mem.Del("first")
	mem.Del("second")
	data, ok, err = mem.Get("first")
	equal(nil, err)
	equal(false, ok)
	_, ok, err = mem.Get("second")
	equal(nil, err)
	equal(false, ok)
}

func TestMemTimerStore(t *testing.T) {

	equal := func(expected, got interface{}) {
		if got != expected {
			t.Fatalf("expeted: %v, got: %v", expected, got)
		}
	}

	m := NewMemProvider()
	RegisterProvider("mem", m)
	p, ok := providerMgr["mem"]
	equal(true, ok)
	equal(m, p)

	store, err := NewTimerStore("Test", "mem", 1*time.Second, func(key string, val string) {
		fmt.Printf("key: %s, value: %v expired\n", key, string(val))
	})
	equal(nil, err)

	store.Set("first", ("this is the first"), 5)
	store.Set("second", ("this is the second"), 6)
	store.Set("second.5", ("this is the second.5"), 6)
	store.Set("third", ("this is the third"), 8)

	var data string

	data, ok, err = store.Get("first")
	equal(nil, err)
	equal(true, ok)
	equal("this is the first", string(data))

	store.Set("second", ("this is the second replacement"), 1)

	time.Sleep(7 * time.Second)

	_, ok, err = store.Get("seond")
	equal(false, ok)
	data, ok, err = store.Get("third")
	equal(true, ok)
	equal("this is the third", string(data))

}

func BenchmarkMemIterate10K(b *testing.B) {
	benchmarkMemIterate(10000, b)
}

func BenchmarkMemIterate100K(b *testing.B) {
	benchmarkMemIterate(100000, b)
}

func BenchmarkMemIterate1M(b *testing.B) {
	benchmarkMemIterate(1000000, b)
}

func benchmarkMemIterate(i int, b *testing.B) {
	for j := 0; j < b.N; j++ {
		mem := TimerStore{
			prefix: "Test",
			store:  NewMemProvider(),
			h:      func(key string, val string) {},
		}
		for n := 0; n < i; n++ {
			key := fmt.Sprintf("key_%d", n)
			val := string(fmt.Sprintf("val_%d", n))
			mem.Set(key, val, int64(n))
		}

		st := time.Now()
		due, ok, _ := mem.store.Before(time.Now().Unix())
		if ok {
			for key, val := range due {
				mem.h(key, val)
				mem.store.Del(key)
			}
		}
		cost := time.Since(st) / time.Microsecond
		fmt.Printf("i: %d, cost: %d\n", i, cost)
	}
}

// func BenchmarkMemTimerStoreParallel(b *testing.B) {
// 	initMem()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			setAndGet()
// 		}
// 	})
// }

var memStore *TimerStore

func initMem() {
	m := NewMemProvider()
	RegisterProvider("mem", m)
	memStore, _ = NewTimerStore("Test", "mem", 1*time.Second, func(key string, val string) {
		// fmt.Printf("key: %s, value: %v expired\n", key, val)
	})
}

func setAndGet() {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s).Intn(1000)
	r1 := rand.New(s).Intn(1000000)
	key := fmt.Sprintf("key_%d", r1)
	val := string(fmt.Sprintf("val_%d", r1))
	memStore.Set(key, val, int64(r))
	memStore.Get(key)
}
