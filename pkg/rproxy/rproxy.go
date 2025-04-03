package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/valyala/fasthttp"
)

type Status uint32

const (
	StatusOK Status = iota
	StatusAccepted
	StatusNotFound
	StatusError
)

type FunctionInstance struct {
	address   string
	Cid       string
	InUse     bool
	LastUsed  time.Time
	DestroyMe bool
	Mu        *sync.Mutex
}

type RProxy struct {
	Hosts  map[string][]FunctionInstance
	c      *fasthttp.Client
	hl     sync.RWMutex
	status map[string]int
}

func New() *RProxy {
	return &RProxy{
		Hosts: make(map[string][]FunctionInstance),
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

func (r *RProxy) Add(name string, ips []util.IpWrapper) error {
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
		ips[i] = util.IpWrapper{Ip: fmt.Sprintf("http://%s:8000/fn", ip.Ip), Cid: ip.Cid}
	}
	fns := make([]FunctionInstance, 0)
	for _, ip := range ips {
		fns = append(fns, FunctionInstance{address: ip.Ip, Cid: ip.Cid, LastUsed: time.Now(), InUse: false, Mu: &sync.Mutex{}, DestroyMe: false})
	}
	r.Hosts[name] = fns

	return nil
}

func (r *RProxy) AddTFInstance(ip string, body []byte) error {
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
		if _, keyInMap := r.Hosts[name]; !keyInMap {
			r.Hosts[name] = make([]FunctionInstance, 0)
		}
		fns := make([]FunctionInstance, 0)
		for _, val := range values {
			fns = append(fns, FunctionInstance{address: val, LastUsed: time.Now(), InUse: false, Mu: &sync.Mutex{}, DestroyMe: false})
		}
		r.Hosts[name] = append(r.Hosts[name], fns...)
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
	outboundIp := GetOutboundIP().String()
	tmp := make(map[string][]string)
	for k, v := range r.Hosts {
		tmp[k] = make([]string, 0)
		for range v {
			tmp[k] = append(tmp[k], fmt.Sprintf("http://%s:8000/%s", outboundIp, k))
		}
	}
	jsonStr, err := json.Marshal(tmp)
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("unable to marshal function register")
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/", addr), bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Fatal(err)
		return fmt.Errorf("unable to perform post request")
	}
	req.Header.Set("X-tinyFaaS-register", outboundIp)
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

	if _, ok := r.Hosts[name]; !ok {
		return fmt.Errorf("function not found")
	}

	delete(r.Hosts, name)
	return nil
}

func (r *RProxy) RemoveIdleInstances() error {
	for _, v := range r.Hosts {
		for _, fn := range v {
			if fn.DestroyMe {
				// TODO: remove this function
			}
		}
	}
	return nil
}

func (r *RProxy) fastCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {

	handler, ok := r.Hosts[name]

	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)

	r.hl.Lock()
	defer r.hl.Unlock()

	if _, keyInMap := r.Hosts[name]; !keyInMap {
		r.status[name] = -1
	}
	var resp = &fasthttp.Response{}
	r.status[name] = (r.status[name] + 1) % len(r.Hosts[name])
	for {
		handler[r.status["name"]].Mu.Lock()
		defer handler[r.status["name"]].Mu.Unlock()
		handler[r.status["name"]].InUse = true
		h := handler[r.status["name"]] // TODO: only chose handlers that are not processing stuff right now, otherwise start a new container and handle this request

		log.Printf("chosen handler: %s", h.address)

		req := &fasthttp.Request{}

		req.SetRequestURI(h.address)
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
		req.SetBodyRaw(payload)

		resp = &fasthttp.Response{}

		err := r.c.DoTimeout(req, resp, 100*time.Second)

		if err != nil {
			if len(r.Hosts[name]) < 1 {
				log.Print(err)
				return StatusError, nil
			}
			r.Hosts[name] = append(r.Hosts[name][:r.status[name]], r.Hosts[name][r.status[name]+1:]...)
			r.status[name] = r.status[name] % len(r.Hosts[name])
			continue
		}
		break
	}
	statusCode := resp.StatusCode()
	respBody := resp.Body()

	if statusCode != http.StatusOK {
		log.Printf("handler returned status code: %d", statusCode)

		return StatusError, nil
	}

	// log.Printf("have response for sync request: %s", respBody)
	handler[r.status["name"]].InUse = false
	handler[r.status["name"]].LastUsed = time.Now()
	return StatusOK, respBody
}

func (r *RProxy) normalCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	handler, ok := r.Hosts[name]

	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)

	r.hl.Lock()
	defer r.hl.Unlock()

	if _, keyInMap := r.Hosts[name]; !keyInMap {
		r.status[name] = -1
	}
	r.status[name] = (r.status[name] + 1) % len(r.Hosts[name])
	h := handler[r.status["name"]]

	log.Printf("chosen handler: %s", h.address)

	req, err := http.NewRequest("POST", h.address, bytes.NewBuffer(payload))

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
