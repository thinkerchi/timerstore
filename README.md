# timerstore
A key-value storage with ttl

Timerstore's feature:
- set a ttl for the key-val store
- set a callback function which would be called when ttl is expired

Timerstore has two types of implementation: memory and redis

## Example for memory:
```
  // create a memory provider
  m := NewMemProvider() 
  
  // register the memory provider
  RegisterProvider("mem", m)  
  
  // create a timer store with cycle interval is 1 second
  store, _ := NewTimerStore("Test", "mem", 1*time.Second, func(key string, val string) {
    fmt.Printf("key: %s, value: %v expired\n", key, string(val))
    // put your callback code here
  })
  
  // Set a pair of key-value
  store.Set("hello", ("world"), 5) // this would be expired in 5 seconds
  
  // Get the value of the key
  data, ok, err := store.Get("hello")
  
  fmt.Printf("data: %v, ok: %v, err: %v\n", data, ok, err")
```

## Example for redis:
```
// redis configuration
config := &Config{
		Host:        "",
		Port:        "",
		Password:    "",
		Type:        "cluster",
		PoolSize:    10,
		PoolTimeout: 10,
	}

  // create a redis provider
  r, _ := NewRedisProvider(config)

  // register the redis provider
  RegisterProvider("redis", r)
  
  // create a timer store with cycle interval is 1 second
  store, _ := NewTimerStore("Test", "redis", 1*time.Second, func(key string, val string) {
    fmt.Printf("Call back--->> now: %v,  key: %s, val: %v\n", time.Now().Unix(), key, val)
    // put your callback code here
  })
  
  // Set a pair of key-value
  store.Set("hello", ("world"), 5) // this would be expired in 5 seconds
  
  // Get the value of the key
  data, ok, err := store.Get("hello")
  
  fmt.Printf("data: %v, ok: %v, err: %v\n", data, ok, err")
```

Error handling is ignored in the examples.
