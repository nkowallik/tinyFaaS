package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

type Status uint32

const (
	StatusOK Status = iota
	StatusAccepted
	StatusNotFound
	StatusError
)

type RProxy struct {
	hosts  map[string][]string
	c      *fasthttp.Client
	hl     sync.RWMutex
	status map[string]int
}

func New() *RProxy {
	return &RProxy{
		hosts: make(map[string][]string),
		c: &fasthttp.Client{
			MaxConnsPerHost:        256 * 1024,
			DisablePathNormalizing: true,
			// increase DNS cache time to an hour instead of default minute
			DialTimeout: (&fasthttp.TCPDialer{
				Concurrency: 0,
			}).DialTimeout,
		},
	}
}

func (r *RProxy) Add(name string, ips []string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	r.hl.Lock()
	defer r.hl.Unlock()

	// if function exists, we should update!
	// if _, ok := r.hosts[name]; ok {
	// 	return fmt.Errorf("function already exists")
	// }

	for i, ip := range ips {
		ips[i] = fmt.Sprintf("http://%s:8000/fn", ip)
	}
	r.hosts[name] = ips

	return nil
}

func (r *RProxy) AddInstance(ip string, body []byte) error {
	if ip == "" {
		return fmt.Errorf("no empty instances")
	}

	r.hl.Lock()
	defer r.hl.Unlock()
	var connections = make(map[string][]string)
	err := json.Unmarshal(body, &connections)
	if err != nil {
		return fmt.Errorf("unable to unmarshal body")
	}
	log.Println(connections)
	for name, values := range connections {
		if _, keyInMap := r.hosts[name]; !keyInMap {
			r.hosts[name] = make([]string, 0)
		}
		r.hosts[name] = append(r.hosts[name], values...)
	}
	return nil
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func (r *RProxy) RegisterInCluster(addr string) error {
	log.Println(addr)
	jsonStr, err := json.Marshal(r.hosts)
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("unable to marshal function register")
	}
	req, err := http.NewRequest("POST", addr, bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("unable to perform post request")
	}
	req.Header.Set("X-tinyFaaS-register", GetOutboundIP().String())
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	return nil
}

func (r *RProxy) Del(name string) error {
	r.hl.Lock()
	defer r.hl.Unlock()

	if _, ok := r.hosts[name]; !ok {
		return fmt.Errorf("function not found")
	}

	delete(r.hosts, name)
	return nil
}

func (r *RProxy) fastCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {

	handler, ok := r.hosts[name]

	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)

	// choose random handler
	r.hl.Lock()
	defer r.hl.Unlock()

	if _, keyInMap := r.hosts[name]; !keyInMap {
		r.status[name] = -1
	}
	r.status[name] = (r.status[name] + 1) % len(r.hosts[name])
	h := handler[r.status["name"]]

	log.Printf("chosen handler: %s", h)

	// req := fasthttp.AcquireRequest()
	req := &fasthttp.Request{}
	req.SetRequestURI(h)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
	req.SetBodyRaw(payload)

	// resp := fasthttp.AcquireResponse()
	resp := &fasthttp.Response{}

	err := r.c.DoTimeout(req, resp, 100*time.Second)
	// fasthttp.ReleaseRequest(req)
	// defer fasthttp.ReleaseResponse(resp)

	if err != nil {
		log.Print(err)
		return StatusError, nil
	}

	statusCode := resp.StatusCode()
	respBody := resp.Body()

	if statusCode != http.StatusOK {
		log.Printf("handler returned status code: %d", statusCode)

		return StatusError, nil
	}

	// log.Printf("have response for sync request: %s", respBody)

	return StatusOK, respBody
}

func (r *RProxy) normalCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	handler, ok := r.hosts[name]

	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)

	// choose random handler
	h := handler[rand.Intn(len(handler))]

	// log.Printf("chosen handler: %s", h)

	req, err := http.NewRequest("POST", h, bytes.NewBuffer(payload))

	if err != nil {
		log.Print(err)
		return StatusError, nil
	}
	for k, v := range headers {
		cleanedKey := cleanHeaderKey(k) // remove special chars from key
		req.Header.Set(cleanedKey, v)
	}

	// call function asynchronously
	if async {
		log.Printf("async request accepted")
		go func() {
			resp, err2 := http.DefaultClient.Do(req)
			if err2 != nil {
				return
			}
			resp.Body.Close()
			log.Printf("async request finished")
		}()
		return StatusAccepted, nil
	}

	// log.Printf("sync request starting")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Print(err)
		return StatusError, nil
	}

	// log.Printf("sync request finished")

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Print(err)
		return StatusError, nil
	}

	// log.Printf("have response for sync request: %s", res_body)

	return StatusOK, res_body
}
func (r *RProxy) Call(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	return r.fastCall(name, payload, async, headers)
	// return r.normalCall(name, payload, async, headers)
}

func cleanHeaderKey(key string) string {
	// a regex pattern to match special characters
	re := regexp.MustCompile(`[:()<>@,;:\"/[\]?={} \t]`)
	// Replace special characters with an empty string
	return re.ReplaceAllString(key, "")
}
