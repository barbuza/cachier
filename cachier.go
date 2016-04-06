package main

import (
	"bytes"
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	"gopkg.in/redis.v3"
)

var logger = logging.MustGetLogger("cachier")
var format = logging.MustStringFormatter(
	`%{color}%{time:15:04:05.000} ▶ %{level:.4s} %{color:reset} %{message}`,
)

// ProxyOptions ...
type ProxyOptions struct {
	ValuePrefix   string
	LockPrefix    string
	TopicName     string
	CookieName    string
	BackendURL    string
	RenderTimeout time.Duration
	CacheDuration time.Duration
	StatsInterval time.Duration
	LogLevel      int
}

// Proxy ...
type Proxy struct {
	PubSub        *redis.PubSub
	RedisClient   *redis.Client
	Mutex         *sync.Mutex
	Channels      map[string]*list.List
	Options       *ProxyOptions
	StatsTicker   *time.Ticker
	reverse       *httputil.ReverseProxy
	httpClient    *http.Client
	backendURL    *url.URL
	encoderHandle codec.Handle
}

type proxyListener struct {
	URL     string
	Done    chan renderEvent
	Element *list.Element
}

type renderResult struct {
	Result []byte `codec:"r,omitempty"`
	Status int    `codec:"s,omitempty"`
}

type renderEvent struct {
	URL      string `codec:"u,omitempty"`
	Location string `codec:"l,omitempty"`
	Status   int    `codec:"s,omitempty"`
}

// NewProxy ...
func NewProxy(redisClient *redis.Client, options *ProxyOptions) *Proxy {
	if len(options.BackendURL) == 0 {
		logger.Panic("BackendURL is empty")
	}
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

	logger.Infof("init %v", options)

	pubsub := redisClient.PubSub()
	pubsub.Subscribe(options.TopicName)

	backendURL, err := url.Parse(options.BackendURL)
	if err != nil {
		log.Panic(err)
	}

	proxy := &Proxy{
		Channels:      make(map[string]*list.List),
		RedisClient:   redisClient,
		PubSub:        pubsub,
		Mutex:         &sync.Mutex{},
		Options:       options,
		StatsTicker:   ticker,
		backendURL:    backendURL,
		reverse:       httputil.NewSingleHostReverseProxy(backendURL),
		httpClient:    &http.Client{},
		encoderHandle: new(codec.JsonHandle),
	}

	if ticker != nil {
		go func() {
			for range ticker.C {
				stats := proxy.GetStats()
				logger.Info("stats:", stats)
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
		logger.Panic(err)
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

func (proxy Proxy) closeListener(listener *proxyListener) {
	proxy.lock()
	defer proxy.unlock()

	old := proxy.Channels[listener.URL]
	old.Remove(listener.Element)
	if old.Len() == 0 {
		delete(proxy.Channels, listener.URL)
	}
	close(listener.Done)
}

func (proxy Proxy) makeListener(url string) *proxyListener {
	proxy.lock()
	defer proxy.unlock()

	channel := make(chan renderEvent)
	old, ok := proxy.Channels[url]
	var element *list.Element
	if ok {
		element = old.PushBack(channel)
	} else {
		new := list.New()
		element = new.PushBack(channel)
		proxy.Channels[url] = new
	}
	return &proxyListener{
		URL:     url,
		Done:    channel,
		Element: element,
	}
}

func (proxy Proxy) writeResult(writer http.ResponseWriter, result renderResult) {
	if result.Status == 503 {
		writer.WriteHeader(503)
		writer.Write([]byte("503"))
	} else {
		writer.WriteHeader(result.Status)
		writer.Write(result.Result)
	}
}

func (proxy Proxy) makeBackendRequest(request *http.Request) renderResult {
	var result renderResult
	backendURL := url.URL{
		Scheme:   proxy.backendURL.Scheme,
		Host:     proxy.backendURL.Host,
		Path:     request.URL.Path,
		RawQuery: request.URL.RawQuery,
	}
	response, err := proxy.httpClient.Get(backendURL.String())
	if err != nil {
		result.Status = 503
	} else {
		result.Status = response.StatusCode
		bytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			result.Status = 503
		} else {
			result.Result = bytes
		}
	}
	return result
}

func (proxy Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	url := request.URL.RequestURI()

	{
		passthrough := request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodOptions

		if !passthrough {
			if len(proxy.Options.CookieName) != 0 {
				if _, err := request.Cookie(proxy.Options.CookieName); err != nil {
					passthrough = true
				}
			}
		}

		if passthrough {
			proxy.reverse.ServeHTTP(writer, request)
			return
		}
	}

	lockKey := proxy.Options.LockPrefix + url
	valueKey := proxy.Options.ValuePrefix + url
	lockVal := proxy.makeLockVal()

	var result renderResult

	listener := proxy.makeListener(url)

	cachedBytes, err := proxy.RedisClient.Get(valueKey).Bytes()

	if err == nil {
		proxy.closeListener(listener)
		codec.NewDecoderBytes(cachedBytes, proxy.encoderHandle).MustDecode(&result)

	} else {

		aquired, err := proxy.RedisClient.SetNX(lockKey, lockVal, proxy.Options.RenderTimeout).Result()
		proxy.closeListener(listener)

		if err != nil {
			logger.Panic(err)
		}

		if aquired {
			logger.Debugf("render %q", url)
			result = proxy.makeBackendRequest(request)

			buffer := bytes.NewBuffer([]byte{})
			encoder := codec.NewEncoder(buffer, proxy.encoderHandle)
			encoder.MustEncode(result)
			logger.Debugf("res=%v", buffer)

			pipeline := proxy.RedisClient.Pipeline()
			if result.Status == 200 {
				pipeline.Set(valueKey, buffer.Bytes(), proxy.Options.CacheDuration)
			}
			pipeline.Del(lockKey)
			_, err := pipeline.Exec()

			if err != nil {
				logger.Panic(err)
			}

			eventBuffer := bytes.NewBuffer([]byte{})
			codec.NewEncoder(eventBuffer, proxy.encoderHandle).MustEncode(renderEvent{
				URL:    url,
				Status: result.Status,
			})
			logger.Debugf("publish %v", eventBuffer)
			if err := proxy.RedisClient.Publish(proxy.Options.TopicName, eventBuffer.String()).Err(); err != nil {
				logger.Panic(err)
			}

		} else {
			select {
			case event := <-listener.Done:
				if event.Status == 200 {
					cachedBytes, err := proxy.RedisClient.Get(valueKey).Bytes()
					if err != nil {
						logger.Panic(err)
					}
					codec.NewDecoderBytes(cachedBytes, proxy.encoderHandle).MustDecode(&result)
				} else {
					result = renderResult{
						Status: event.Status,
					}
				}

			case <-time.After(proxy.Options.RenderTimeout):
				result = renderResult{
					Status: 503,
				}
				proxy.closeListener(listener)
			}
		}
	}

	proxy.writeResult(writer, result)
}

func (proxy Proxy) handleRenderMessage(event renderEvent) {
	proxy.lock()
	defer proxy.unlock()

	channels, ok := proxy.Channels[event.URL]
	if ok {
		logger.Debugf("%q ready", event.URL)
		for e := channels.Front(); e != nil; e = e.Next() {
			channel := e.Value.(chan renderEvent)
			channel <- event
			close(channel)
		}
		delete(proxy.Channels, event.URL)
	}
}

// GetStats ...
func (proxy Proxy) GetStats() map[string]int {
	proxy.lock()
	defer proxy.unlock()
	res := make(map[string]int)
	for url := range proxy.Channels {
		res[url] = proxy.Channels[url].Len()
	}
	return res
}

func (proxy Proxy) readRenderEvents() {
	for {
		msg, err := proxy.PubSub.ReceiveMessage()
		if err != nil {
			logger.Panic(err)
		}
		var event renderEvent
		codec.NewDecoderBytes([]byte(msg.Payload), proxy.encoderHandle).MustDecode(&event)
		proxy.handleRenderMessage(event)
	}
}

func main() {
	var redisHost string
	var redisPort int
	var redisDB int64
	var backendURL string
	var bind string
	var statsInterval int64
	var logLevel string

	flag.StringVar(&backendURL, "backendURL", "http://127.0.0.1:3000", "backend url")
	flag.StringVar(&redisHost, "redisHost", "127.0.0.1", "redis host")
	flag.IntVar(&redisPort, "redisPort", 6379, "redis port")
	flag.Int64Var(&redisDB, "redisDB", 0, "redis database to use (default 0)")
	flag.StringVar(&bind, "bind", ":8080", "host:port or :port to listen")
	flag.Int64Var(&statsInterval, "statsInterval", 0, "dump stats interval, ms (default disabled)")
	flag.StringVar(&logLevel, "logLevel", "WARNING", "log level (DEBUG, INFO, WARNING, CRITICAL)")
	flag.Parse()

	logBackend := logging.NewBackendFormatter(logging.NewLogBackend(os.Stderr, "", 0), format)
	leveled := logging.AddModuleLevel(logBackend)
	leveled.SetLevel(logging.CRITICAL, "cachier")
	if logLevel == "DEBUG" {
		leveled.SetLevel(logging.DEBUG, "cachier")
	} else if logLevel == "INFO" {
		leveled.SetLevel(logging.INFO, "cachier")
	} else if logLevel == "WARNING" {
		leveled.SetLevel(logging.WARNING, "cachier")
	} else if logLevel == "CRITICAL" {
		leveled.SetLevel(logging.CRITICAL, "cachier")
	}
	logger.SetBackend(leveled)

	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%v:%v", redisHost, redisPort),
		DB:   redisDB,
	})
	_, err := client.Ping().Result()
	if err != nil {
		logger.Panic(err)
	}

	proxy := NewProxy(client, &ProxyOptions{
		BackendURL:    backendURL,
		StatsInterval: time.Duration(statsInterval) * time.Millisecond,
	})

	logger.Infof("listen %v", bind)
	http.ListenAndServe(bind, proxy)
}
