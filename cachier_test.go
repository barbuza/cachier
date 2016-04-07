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
		time.Sleep(time.Millisecond * time.Duration(1000+rnd.Intn(1000)))
		writer.WriteHeader(200)
		writer.Write([]byte("response=" + request.URL.RequestURI()))
	}))

	proxy := NewProxy(client, &ProxyOptions{
		BackendURL:    backend.URL,
		CookieName:    "foo",
		StatsInterval: time.Millisecond * 100,
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

	clients := 400
	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		url := fmt.Sprintf("/%v", rnd.Intn(clients>>4))
		sleep := time.Millisecond * time.Duration(rnd.Intn(2000))
		go func() {
			defer wg.Done()
			time.Sleep(sleep)

			client := &http.Client{
				Timeout: time.Second * 5,
			}
			request, err := http.NewRequest(http.MethodGet, server.URL+url, bytes.NewBuffer([]byte{}))
			if err != nil {
				log.Panic(err)
			}

			request.AddCookie(&http.Cookie{
				Name:  "foo2",
				Value: "bar",
			})
			res, httpErr := client.Do(request)

			if httpErr == nil {
				body, readErr := ioutil.ReadAll(res.Body)
				if readErr == nil {
					assert.Equal(t, "response="+url, string(body), "response")
				} else {
					assert.Nil(t, readErr, "read error")
				}
			} else {
				assert.Nil(t, httpErr, "http client error")
			}
		}()
	}

	wg.Wait()

	assert.Empty(t, proxy.GetStats(), "no waiting handlers")
}
