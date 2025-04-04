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

type Cluster struct {
	Nodes map[string]ClusterNode
	Size  uint64
	Mu    *sync.Mutex
	Ip    string
}

type ClusterNode struct {
	Address  string
	CpuUsage float64
	Running  map[string]int16
	InUse    map[string]int16
	Up       bool
	LastUsed time.Time
	Master   bool
}

type FunctionInstance struct {
	address  string
	Cid      string
	InUse    bool
	LastUsed time.Time
	Mu       *sync.Mutex
}

type RProxy struct {
	Hosts   map[string]map[string]FunctionInstance
	c       *fasthttp.Client
	Hl      sync.RWMutex
	Cluster Cluster
}

func New() *RProxy {
	nodes := make(map[string]ClusterNode, 1)
	addr := GetOutboundIP().String()
	nodes[addr] = ClusterNode{
		Address:  addr,
		CpuUsage: -1.0,
		Running:  make(map[string]int16),
		InUse:    make(map[string]int16),
		Up:       true,
		LastUsed: time.Now(),
		Master:   true,
	}
	return &RProxy{
		Hosts: make(map[string]map[string]FunctionInstance),
		c: &fasthttp.Client{
			MaxConnsPerHost:        256 * 1024,
			DisablePathNormalizing: true,
			// increase DNS cache time to an hour instead of default minute
			DialTimeout: (&fasthttp.TCPDialer{
				Concurrency: 0,
			}).DialTimeout,
		},
		Cluster: Cluster{
			Nodes: nodes,
			Size:  1,
			Mu:    &sync.Mutex{},
			Ip:    GetOutboundIP().String(),
		},
	}
}

func (r *RProxy) Add(name string, ips []util.IpWrapper) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}
	r.Hl.Lock()
	defer r.Hl.Unlock()

	// if function exists, we should update!
	// if _, ok := r.hosts[name]; ok {
	// 	return fmt.Errorf("function already exists")
	// }

	for i, ip := range ips {
		ips[i] = util.IpWrapper{Ip: fmt.Sprintf("http://%s:8000/fn", ip.Ip), Cid: ip.Cid}
	}
	for _, ip := range ips {
		_, ok := r.Hosts[name]
		if !ok {
			r.Hosts[name] = make(map[string]FunctionInstance)
		}
		r.Hosts[name][ip.Cid] = FunctionInstance{address: ip.Ip, Cid: ip.Cid, LastUsed: time.Now(), InUse: false, Mu: &sync.Mutex{}}
	}

	return nil
}

func (r *RProxy) UpdateClusterNode(nodeAddr string, values []byte) error {
	msg := struct {
		CpuUsage float64          `json:"cpuUsage"`
		Running  map[string]int16 `json:"running"`
	}{}

	err := json.Unmarshal(values, &msg)
	if err != nil {
		return err
	}
	r.Cluster.Mu.Lock()
	node := r.Cluster.Nodes[nodeAddr]
	defer r.Cluster.Mu.Unlock()
	node.CpuUsage = msg.CpuUsage
	for k, v := range msg.Running {
		node.Running[k] = v
	}
	r.Cluster.Nodes[nodeAddr] = node
	return nil
}

func (r *RProxy) AddTFInstance(ip string, body []byte) error {
	if ip == "" {
		return fmt.Errorf("no empty instances")
	}

	r.Hl.Lock()
	defer r.Hl.Unlock()
	var connections = make(map[string][]string)
	err := json.Unmarshal(body, &connections)
	if err != nil {
		return fmt.Errorf("unable to unmarshal body")
	}
	log.Println(connections)
	/*for name, values := range connections {
		for _, val := range values {
			fns = append(fns, FunctionInstance{address: val, LastUsed: time.Now(), InUse: false, Mu: &sync.Mutex{}})
		}
		r.Hosts[name] = append(r.Hosts[name], fns...)
	}*/
	// TODO: figure out clusters
	return nil
}

func (r *RProxy) GetIP() string {
	return r.Cluster.Ip
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
	outboundIp := r.Cluster.Ip
	msg := struct {
		CpuUsage float32          `json:"cpuUsage"`
		Running  map[string]int16 `json:"running"`
	}{
		CpuUsage: 0.0,
		Running:  r.Cluster.Nodes[outboundIp].Running,
	}
	jsonStr, err := json.Marshal(msg)
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
	r.Hl.Lock()
	defer r.Hl.Unlock()

	if _, ok := r.Hosts[name]; !ok {
		return fmt.Errorf("function not found")
	}

	delete(r.Hosts, name)
	return nil
}

func (r *RProxy) spawnNewInstance(name string, headers map[string]string) (string, string) {
	tmp := struct {
		Name string            `json:"name"`
		Envs map[string]string `json:"envs"`
	}{
		Name: name,
		Envs: headers,
	}
	jsonStr, err := json.Marshal(tmp)
	if err != nil {
		log.Fatal(err)
		//return fmt.Errorf("unable to marshal function register")
	}
	req, err := http.NewRequest("POST", "http://127.0.0.1:8080/coldstart", bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Fatal(err)
		//return fmt.Errorf("unable to perform post request")
	}
	//req.Header.Set("X-tinyFaaS-register", outboundIp)
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Unable to perform request")
		panic(err)
	}
	log.Println(resp.StatusCode)
	resp_body, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Println("Unable to read body")
		panic(err)
	}

	b := struct {
		Ip  string `json:"Ip"`
		Cid string `json:"Cid"`
	}{}
	log.Printf("BODY := '%s'", resp_body)
	err = json.Unmarshal(resp_body, &b)
	if err != nil {
		log.Println("Unable to unmarshal")
		panic(err)
	}
	defer resp.Body.Close()
	// use spawned function to run request (cold-start) (forward to newly created function container)
	return b.Ip, b.Cid

}

func (r *RProxy) fastCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {

	_, ok := r.Hosts[name]

	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	var resp = &fasthttp.Response{}
	var freeCid string
	if len(r.Hosts[name]) == 0 {
		ip, cid := r.spawnNewInstance(name, headers)
		newHandler := FunctionInstance{address: fmt.Sprintf("http://%s:8000/fn", ip), Cid: cid, InUse: false, Mu: &sync.Mutex{}}
		r.Hosts[name][cid] = newHandler
		freeCid = cid
	}
	for cid := range r.Hosts[name] {
		if !r.Hosts[name][cid].InUse {
			h := r.Hosts[name][cid]
			h.InUse = true
			r.Hosts[name][cid] = h
			freeCid = cid
			break
		}
	}
	if freeCid == "" {
		ip, cid := r.spawnNewInstance(name, headers)
		newHandler := FunctionInstance{address: fmt.Sprintf("http://%s:8000/fn", ip), Cid: cid, InUse: true, Mu: &sync.Mutex{}}
		r.Hosts[name][cid] = newHandler
		freeCid = cid
	}
	log.Printf("chosen handler: %s", r.Hosts[name][freeCid].address)

	req := &fasthttp.Request{}

	req.SetRequestURI(r.Hosts[name][freeCid].address)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
	req.SetBodyRaw(payload)

	resp = &fasthttp.Response{}

	err := r.c.DoTimeout(req, resp, 100*time.Second)

	if err != nil {
		log.Println("Unable to complete request, retrying ...")
		return r.fastCall(name, payload, async, headers)
	}

	statusCode := resp.StatusCode()
	respBody := resp.Body()

	if statusCode != http.StatusOK {
		log.Printf("handler returned status code: %d", statusCode)

		return StatusError, nil
	}

	// log.Printf("have response for sync request: %s", respBody)
	log.Println("Attempting write to storage")
	r.Hosts[name][freeCid].Mu.Lock()
	tmp := r.Hosts[name][freeCid]
	tmp.InUse = false
	tmp.LastUsed = time.Now()
	r.Hosts[name][freeCid] = tmp
	r.Hosts[name][freeCid].Mu.Unlock()
	log.Println("Written to storage")
	return StatusOK, respBody
}

func (r *RProxy) normalCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	if len(r.Hosts[name]) > 0 {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)
	var h FunctionInstance
	for cid, fi := range r.Hosts[name] {
		tmpMu := r.Hosts[name][cid].Mu
		if !r.Hosts[name][cid].InUse && tmpMu.TryLock() {
			defer tmpMu.Unlock()
			h = fi
		}
	}

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
