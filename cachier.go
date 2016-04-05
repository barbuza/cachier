package main

import (
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

// ProxyOptions ...
type ProxyOptions struct {
	ValuePrefix   string
	LockPrefix    string
	TopicName     string
	RenderTimeout time.Duration
	CacheDuration time.Duration
	StatsInterval time.Duration
}

// Proxy ...
type Proxy struct {
	PubSub      *redis.PubSub
	RedisClient *redis.Client
	Mutex       *sync.Mutex
	Channels    map[string]*list.List
	Options     *ProxyOptions
	StatsTicker *time.Ticker
}

type proxyListener struct {
	URL     string
	Done    chan bool
	Element *list.Element
}

type renderResult struct {
	Result  string
	Timeout bool
}

// NewProxy ...
func NewProxy(redisClient *redis.Client, options *ProxyOptions) *Proxy {
	if len(options.LockPrefix) == 0 {
		options.LockPrefix = "lock-"
	}
	if len(options.ValuePrefix) == 0 {
		options.ValuePrefix = "value-"
	}
	if len(options.TopicName) == 0 {
		options.TopicName = "cachier"
	}
	if options.RenderTimeout == 0 {
		options.RenderTimeout = 5 * time.Second
	}
	if options.CacheDuration == 0 {
		options.CacheDuration = 5 * time.Second
	}

	var ticker *time.Ticker
	if options.StatsInterval != 0 {
		ticker = time.NewTicker(options.StatsInterval)
	}

	log.Printf("init %q", options)

	pubsub := redisClient.PubSub()
	pubsub.Subscribe(options.TopicName)

	proxy := &Proxy{
		Channels:    make(map[string]*list.List),
		RedisClient: redisClient,
		PubSub:      pubsub,
		Mutex:       &sync.Mutex{},
		Options:     options,
		StatsTicker: ticker,
	}

	if ticker != nil {
		go func() {
			for range ticker.C {
				log.Printf("stats: %v waiting", len(proxy.Channels))
			}
		}()
	}

	go proxy.readRenderEvents()
	return proxy
}

// Close ...
func (proxy Proxy) Close() {
	if proxy.StatsTicker != nil {
		proxy.StatsTicker.Stop()
	}

	if err := proxy.PubSub.Unsubscribe(proxy.Options.TopicName); err != nil {
		log.Panic(err)
	}
}

func (proxy Proxy) lock() {
	proxy.Mutex.Lock()
}

func (proxy Proxy) unlock() {
	proxy.Mutex.Unlock()
}

func (proxy Proxy) makeLockVal() string {
	var bytes = make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func (proxy Proxy) closeListener(listener proxyListener) {
	proxy.lock()
	defer proxy.unlock()

	old := proxy.Channels[listener.URL]
	old.Remove(listener.Element)
	if old.Len() == 0 {
		delete(proxy.Channels, listener.URL)
	}
	close(listener.Done)
}

func (proxy Proxy) makeListener(url string) proxyListener {
	proxy.lock()
	defer proxy.unlock()

	channel := make(chan bool)
	old, ok := proxy.Channels[url]
	var element *list.Element
	if ok {
		element = old.PushBack(channel)
	} else {
		new := list.New()
		element = new.PushBack(channel)
		proxy.Channels[url] = new
	}
	return proxyListener{
		URL:     url,
		Done:    channel,
		Element: element,
	}
}

func (proxy Proxy) writeResult(writer http.ResponseWriter, result renderResult) {
	if result.Timeout {
		writer.WriteHeader(503)
		writer.Write([]byte("503"))
	} else {
		writer.WriteHeader(200)
		writer.Write([]byte(result.Result))
	}
}

func (proxy Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	url := request.URL.RequestURI()

	lockKey := proxy.Options.LockPrefix + url
	valueKey := proxy.Options.ValuePrefix + url
	lockVal := proxy.makeLockVal()

	var result renderResult

	listener := proxy.makeListener(url)
	cached, err := proxy.RedisClient.Get(valueKey).Result()

	if err == nil {
		proxy.closeListener(listener)
		result = renderResult{
			Result: cached,
		}

	} else {

		aquired, err := proxy.RedisClient.SetNX(lockKey, lockVal, proxy.Options.RenderTimeout).Result()

		if err != nil {
			log.Panic(err)
		}

		if aquired {
			proxy.closeListener(listener)

			defer proxy.RedisClient.Del(lockKey)

			log.Printf("render %q", url)
			time.Sleep(1 * time.Second)

			result = renderResult{
				Result: fmt.Sprintf("result=%v", url),
			}

			pipeline := proxy.RedisClient.Pipeline()
			pipeline.Set(valueKey, result.Result, proxy.Options.CacheDuration)
			pipeline.Del(lockKey)
			_, err := pipeline.Exec()

			if err != nil {
				log.Panic(err)
			}

			if err := proxy.RedisClient.Publish(proxy.Options.TopicName, url).Err(); err != nil {
				log.Panic(err)
			}

		} else {
			select {
			case <-listener.Done:
				data, err := proxy.RedisClient.Get(valueKey).Result()
				if err != nil {
					log.Panic(err)
				}
				result = renderResult{
					Result: data,
				}
			case <-time.After(proxy.Options.RenderTimeout):
				result = renderResult{
					Timeout: true,
				}
			}

			proxy.closeListener(listener)
		}
	}

	proxy.writeResult(writer, result)
}

func (proxy Proxy) handleRenderMessage(url string) {
	proxy.lock()
	defer proxy.unlock()

	channels, ok := proxy.Channels[url]
	if ok {
		log.Printf("%q ready", url)
		if ok {
			for e := channels.Front(); e != nil; e = e.Next() {
				e.Value.(chan bool) <- true
			}
		}
	}
}

func (proxy Proxy) readRenderEvents() {
	for {
		msg, err := proxy.PubSub.ReceiveMessage()
		if err != nil {
			log.Panic(err)
		}
		proxy.handleRenderMessage(msg.Payload)
	}
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       2,
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Panic(err)
	}

	proxy := NewProxy(client, &ProxyOptions{})

	log.Println("listen on :8080")
	http.ListenAndServe(":8080", proxy)
}
