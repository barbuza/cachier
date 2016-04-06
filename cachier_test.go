package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/redis.v3"
)

// TestProxy ...
func TestProxy(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       2,
	})

	if err := client.Ping().Err(); err != nil {
		log.Panic(err)
	}

	testKey := fmt.Sprintf("%v", rnd.Intn(10000))

	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(time.Duration(rnd.Intn(1000)))
		writer.Write([]byte("response=" + request.URL.RequestURI()))
	}))

	proxy := NewProxy(client, &ProxyOptions{
		BackendURL:    backend.URL,
		CookieName:    "foo",
		StatsInterval: time.Second,
		RenderTimeout: time.Second * 2,
		CacheDuration: time.Second * 4,
		TopicName:     "cachier-test-" + testKey,
		ValuePrefix:   "value-test-" + testKey,
		LockPrefix:    "lock-test-" + testKey,
	})

	server := httptest.NewServer(proxy)

	defer proxy.Close()
	defer server.Close()
	defer backend.Close()

	clients := 4000
	var wg sync.WaitGroup
	wg.Add(clients)

	failed := 0

	for i := 0; i < clients; i++ {
		url := fmt.Sprintf("/%v", rnd.Intn(clients>>4))
		sleep := time.Duration(rnd.Intn(2000))
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * sleep)

			client := &http.Client{}
			request, err := http.NewRequest(http.MethodGet, server.URL+url, bytes.NewBuffer([]byte{}))
			if err != nil {
				log.Panic(err)
			}

			request.AddCookie(&http.Cookie{
				Name:  "foo",
				Value: "bar",
			})
			res, err := client.Do(request)

			// res, err := http.Get(server.URL + url)

			if err == nil {
				body, err := ioutil.ReadAll(res.Body)
				if err == nil {
					assert.Equal(t, "response="+url, fmt.Sprintf("%s", body), "response")
				} else {
					failed++
				}
			} else {
				failed++
			}
		}()
	}

	assert.Equal(t, 0, failed, "no failed requests")

	wg.Wait()
	assert.Empty(t, proxy.GetStats(), "no waiting handlers")
}
