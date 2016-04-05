package main

import (
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

	proxy := NewProxy(client, &ProxyOptions{
		TopicName:   "cachier-test-" + testKey,
		ValuePrefix: "value-test-" + testKey,
		LockPrefix:  "lock-test-" + testKey,
	})

	server := httptest.NewServer(proxy)

	defer proxy.Close()
	defer server.Close()

	clients := 500
	var wg sync.WaitGroup
	wg.Add(clients)

	failed := 0

	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * time.Duration(rnd.Intn(500)))

			url := fmt.Sprintf("/%v", rnd.Intn(clients>>4))
			res, err := http.Get(server.URL + url)

			if err == nil {
				body, err := ioutil.ReadAll(res.Body)
				if err == nil {
					assert.Equal(t, "result="+url, fmt.Sprintf("%s", body), "response")
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
}
