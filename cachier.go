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
	Done    chan *renderEvent
	Element *list.Element
}

type renderResult struct {
	Result []byte
	Status int
}

type renderEvent struct {
	URL      string `codec:"u"`
	Location string `codec:"l"`
	Status   int    `codec:"s"`
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
		Channels:    make(map[string]*list.List),
		RedisClient: redisClient,
		PubSub:      pubsub,
		Mutex:       &sync.Mutex{},
		Options:     options,
		StatsTicker: ticker,
		backendURL:  backendURL,
		reverse:     httputil.NewSingleHostReverseProxy(backendURL),
		httpClient: &http.Client{
			Timeout: options.RenderTimeout,
		},
		encoderHandle: &codec.MsgpackHandle{},
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

	channel := make(chan *renderEvent)
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

func (proxy Proxy) writeResult(writer http.ResponseWriter, result *renderResult) {
	if result.Status == 0 {
		logger.Critical("result status is 0")
	}
	if result.Status == 503 {
		writer.WriteHeader(503)
		writer.Write([]byte("503"))
	} else {
		writer.WriteHeader(result.Status)
		writer.Write(result.Result)
	}
}

func (proxy Proxy) makeBackendRequest(request *http.Request, result *renderResult) {
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
		result.Result, err = ioutil.ReadAll(response.Body)
		if err != nil {
			result.Status = 503
		}
	}
}

func (proxy Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	url := request.URL.RequestURI()

	{
		passthrough := request.Method != http.MethodGet && request.Method != http.MethodHead && request.Method != http.MethodOptions

		if !passthrough {
			if len(proxy.Options.CookieName) != 0 {
				if _, err := request.Cookie(proxy.Options.CookieName); err == nil {
					passthrough = true
				}
			}
		}

		if passthrough {
			logger.Debugf("passthrough %s", url)
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
		logger.Debugf("cached %s", url)
		proxy.closeListener(listener)
		codec.NewDecoderBytes(cachedBytes, proxy.encoderHandle).MustDecode(&result)
	} else {

		aquired, err := proxy.RedisClient.SetNX(lockKey, lockVal, proxy.Options.RenderTimeout).Result()

		if err != nil {
			logger.Panic(err)
		}

		if aquired {
			proxy.closeListener(listener)
			logger.Debugf("render %s", url)
			proxy.makeBackendRequest(request, &result)

			buffer := bytes.NewBuffer([]byte{})
			encoder := codec.NewEncoder(buffer, proxy.encoderHandle)
			encoder.MustEncode(result)

			if result.Status == 200 {
				if err := proxy.RedisClient.Set(valueKey, buffer.Bytes(), proxy.Options.CacheDuration).Err(); err != nil {
					logger.Panic(err)
				}
			}

			if err := proxy.RedisClient.Del(lockKey).Err(); err != nil {
				logger.Panic(err)
			}

			eventBuffer := bytes.NewBuffer([]byte{})
			event := renderEvent{
				URL:    url,
				Status: result.Status,
			}
			codec.NewEncoder(eventBuffer, proxy.encoderHandle).MustEncode(&event)
			logger.Debugf("render %s done", url)
			if err := proxy.RedisClient.Publish(proxy.Options.TopicName, eventBuffer.String()).Err(); err != nil {
				logger.Panic(err)
			}

		} else {
			logger.Debugf("wait for %s", url)
			select {
			case event := <-listener.Done:
				logger.Debugf("resolved %s", url)
				if event.Status == 200 {
					cachedBytes, err := proxy.RedisClient.Get(valueKey).Bytes()
					if err != nil {
						logger.Panic(err)
					}
					codec.NewDecoderBytes(cachedBytes, proxy.encoderHandle).MustDecode(&result)
				} else {
					result = renderResult{Status: 503}
				}

			case <-time.After(proxy.Options.RenderTimeout):
				proxy.closeListener(listener)
				logger.Debugf("timeout %s", url)
				result = renderResult{Status: 503}
			}
		}
	}

	proxy.writeResult(writer, &result)
}

func (proxy Proxy) handleRenderMessage(event *renderEvent) {
	proxy.lock()
	defer proxy.unlock()

	channels, ok := proxy.Channels[event.URL]
	if ok {
		logger.Debugf("%s ready", event.URL)
		for e := channels.Front(); e != nil; e = e.Next() {
			channel := e.Value.(chan *renderEvent)
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
		proxy.handleRenderMessage(&event)
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
	var cacheDuration int64
	var renderTimeout int64

	flag.StringVar(&backendURL, "backendURL", "http://127.0.0.1:3000", "backend url")
	flag.StringVar(&redisHost, "redisHost", "127.0.0.1", "redis host")
	flag.IntVar(&redisPort, "redisPort", 6379, "redis port")
	flag.Int64Var(&redisDB, "redisDB", 0, "redis database to use (default 0)")
	flag.StringVar(&bind, "bind", ":8080", "host:port or :port to listen")
	flag.Int64Var(&cacheDuration, "cacheDuration", 5000, "cache duration, ms")
	flag.Int64Var(&renderTimeout, "renderTimeout", 5000, "render timeout, ms")
	flag.Int64Var(&statsInterval, "statsInterval", 0, "dump stats interval, ms (default disabled)")
	flag.StringVar(&logLevel, "logLevel", "WARNING", "log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
	flag.Parse()

	logBackend := logging.NewBackendFormatter(logging.NewLogBackend(os.Stderr, "", 0), format)
	leveled := logging.AddModuleLevel(logBackend)

	levels := map[string]logging.Level{
		"DEBUG":    logging.DEBUG,
		"INFO":     logging.INFO,
		"WARNING":  logging.WARNING,
		"ERROR":    logging.ERROR,
		"CRITICAL": logging.CRITICAL,
	}
	leveled.SetLevel(levels[logLevel], "cachier")

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
		CacheDuration: time.Duration(cacheDuration) * time.Millisecond,
		RenderTimeout: time.Duration(renderTimeout) * time.Millisecond,
		StatsInterval: time.Duration(statsInterval) * time.Millisecond,
	})

	logger.Infof("listen %v", bind)
	http.ListenAndServe(bind, proxy)
}
