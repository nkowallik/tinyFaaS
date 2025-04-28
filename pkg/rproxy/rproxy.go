package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
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
	StatusTooMany
	StatusTimeout
)

type Cluster struct {
	Nodes    map[string]*ClusterNode
	Mu       *sync.RWMutex
	Ip       string
	Worker   bool
	Master   *ClusterNode
	Starting bool
	LastJoin time.Time
	UsageMu  *sync.Mutex
}

type ClusterNode struct {
	Address  string           `json:"address"`
	CpuUsage float64          `json:"cpuusage"`
	RamUsage float64          `json:"ramusage"`
	Running  map[string]int16 `json:"running"`
	InUse    map[string]int16 `json:"used"`
	Up       bool             `json:"up"`
	LastUsed time.Time        `json:"lastused"`
	IsMaster bool             `json:"ismaster"`
}

type FunctionHandler struct {
	Instances map[string]*FunctionInstance
	Mu        *sync.RWMutex
	Starting  int
}

type FunctionInstance struct {
	address  string
	Cid      string
	InUse    bool
	LastUsed time.Time
	Mu       *sync.Mutex
}

type RProxy struct {
	Hosts   map[string]*FunctionHandler
	c       *fasthttp.Client
	Hl      sync.RWMutex
	Cluster *Cluster
}

func New() *RProxy {
	addr := GetOutboundIP().String()
	node := ClusterNode{
		Address:  addr,
		CpuUsage: -1.0,
		RamUsage: -1.0,
		Running:  make(map[string]int16),
		InUse:    make(map[string]int16),
		Up:       true,
		LastUsed: time.Now(),
		IsMaster: true,
	}
	nodes := make(map[string]*ClusterNode, 1)
	nodes[addr] = &node
	return &RProxy{
		Hosts: make(map[string]*FunctionHandler),
		c: &fasthttp.Client{
			MaxConnsPerHost:        256 * 1024,
			DisablePathNormalizing: true,
			// increase DNS cache time to an hour instead of default minute
			DialTimeout: (&fasthttp.TCPDialer{
				Concurrency: 0,
			}).DialTimeout,
		},
		Cluster: &Cluster{
			Nodes:    nodes,
			Mu:       &sync.RWMutex{},
			Ip:       addr,
			Worker:   false,
			Starting: false,
			Master:   &node,
			LastJoin: time.Now(),
			UsageMu:  &sync.Mutex{},
		},
	}
}

func (r *RProxy) Add(name string, ips []util.IpWrapper) error {
	r.Hl.Lock()
	defer r.Hl.Unlock()

	r.Hosts[name] = &FunctionHandler{Instances: make(map[string]*FunctionInstance), Mu: &sync.RWMutex{}}

	for i, ip := range ips {
		ips[i] = util.IpWrapper{Ip: fmt.Sprintf("http://%s:8000/fn", ip.Ip), Cid: ip.Cid}
	}
	for _, ip := range ips {
		r.Hosts[name].Instances[ip.Cid] = &FunctionInstance{address: ip.Ip, Cid: ip.Cid, LastUsed: time.Now(), InUse: false, Mu: &sync.Mutex{}}
	}

	return nil
}

func (r *RProxy) UpdateClusterNode(nodeAddr string, values []byte) error {
	log.Printf("Updating node %s\n", nodeAddr)
	msg := util.StatusMessage{}

	err := json.Unmarshal(values, &msg)
	if err != nil {
		log.Fatalln("Unable to unmarshal!", err)
		return err
	}
	r.Cluster.Mu.Lock()
	defer r.Cluster.Mu.Unlock()
	node, ok := r.Cluster.Nodes[nodeAddr]
	log.Printf("%s \t RAM: %f; CPU: %f", nodeAddr, msg.RamUsage, msg.CpuUsage)
	go r.WriteUsageLog(fmt.Sprintf("%s \t%f;%f", nodeAddr, msg.CpuUsage, msg.RamUsage))
	if !ok {
		r.Cluster.Nodes[nodeAddr] = &ClusterNode{
			Address:  nodeAddr,
			CpuUsage: msg.CpuUsage,
			RamUsage: msg.RamUsage,
			Running:  msg.Running,
			InUse:    make(map[string]int16),
			Up:       true,
			LastUsed: time.Now(),
			IsMaster: false,
		}
	} else {
		node.CpuUsage = msg.CpuUsage
		node.RamUsage = msg.RamUsage
		node.Up = true
		for k, v := range msg.Running {
			node.Running[k] = v
		}
		r.Cluster.Nodes[nodeAddr] = node
	}
	return nil
}

func (r *RProxy) AddTFInstance(ip string, body []byte) (ClusterNode, error) {
	if ip == "" {
		return ClusterNode{}, fmt.Errorf("no empty instances")
	}

	msg := util.StatusMessage{}

	err := json.Unmarshal(body, &msg)
	if err != nil {
		log.Fatalln("Unable to unmarshal!", err)
	}
	go r.WriteClusterLog(fmt.Sprintf("%s joined", ip))
	r.Cluster.Mu.Lock()
	defer r.Cluster.Mu.Unlock()
	r.Cluster.Nodes[ip] = &ClusterNode{
		Address:  ip,
		CpuUsage: msg.CpuUsage,
		RamUsage: msg.RamUsage,
		Running:  make(map[string]int16),
		InUse:    make(map[string]int16),
		Up:       true,
		LastUsed: time.Now(),
		IsMaster: false,
	}
	r.Cluster.LastJoin = time.Now()
	r.Cluster.Starting = false
	return *r.Cluster.Master, nil
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
	log.Printf("Registering in Custer %s", addr)
	outboundIp := r.Cluster.Ip
	cpu_usage := r.Cluster.Nodes[outboundIp].CpuUsage
	ram_usage := r.Cluster.Nodes[outboundIp].RamUsage
	msg := util.StatusMessage{
		CpuUsage: cpu_usage,
		RamUsage: ram_usage,
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
	resp_body, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Println("Unable to read body")
		panic(err)
	}
	tmp := ClusterNode{}
	err = json.Unmarshal(resp_body, &tmp)
	if err != nil {
		log.Println("Unable to unpack ")
		log.Fatal(err)
	}
	r.Cluster.Mu.Lock()
	defer r.Cluster.Mu.Unlock()
	r.Cluster.Master.IsMaster = false
	r.Cluster.Nodes[r.Cluster.Master.Address] = r.Cluster.Master
	r.Cluster.Nodes[tmp.Address] = &tmp
	r.Cluster.Master = r.Cluster.Nodes[tmp.Address]
	r.Cluster.Worker = true
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

func (r *RProxy) StopInstance(cid string, name string) error {
	t := struct {
		Cid string
	}{
		Cid: cid,
	}

	jsonStr, err := json.Marshal(t)
	if err != nil {
		log.Print(err)
		return err
	}
	counter := 0
	for {
		counter++
		req := &fasthttp.Request{}
		req.SetRequestURI("http://127.0.0.1:8080/rminstance")
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
		req.SetBodyRaw(jsonStr)
		resp := fasthttp.Response{}
		err = r.c.DoTimeout(req, &resp, 100*time.Second)
		if err != nil {
			log.Print(err)
			continue
		}
		if resp.StatusCode() < 400 {
			r.Hl.Lock()
			delete(r.Hosts[name].Instances, cid)
			r.Hl.Unlock()
			break
		}
		if counter > 4 {
			log.Println("Stop trying to delete")
			return fmt.Errorf("unable to delete instance")
		}
	}
	return nil
}

func (r *RProxy) spawnNewInstance(name string, headers map[string]string) (string, string, error) {
	r.Hosts[name].Mu.Lock()
	r.Hosts[name].Starting += 1
	r.Hosts[name].Mu.Unlock()
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
		r.Hosts[name].Mu.Lock()
		r.Hosts[name].Starting -= 1
		r.Hosts[name].Mu.Unlock()
		return "", "", err
		//return fmt.Errorf("unable to marshal function register")
	}

	req := &fasthttp.Request{}
	req.SetRequestURI("http://127.0.0.1:8080/coldstart")
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
	req.SetBodyRaw(jsonStr)

	resp := &fasthttp.Response{}

	err = r.c.DoTimeout(req, resp, 100*time.Second)

	//req, err := http.NewRequest("POST", "http://127.0.0.1:8080/coldstart", bytes.NewBuffer(jsonStr))
	//req.Header.Set("X-tinyFaaS-register", outboundIp)
	//req.Header.Set("Content-Type", "application/json")

	//client := http.Client{}
	//resp, err := client.Do(req)
	if err != nil {
		log.Println("Unable to perform request")
		log.Println(err)
		r.Hosts[name].Mu.Lock()
		r.Hosts[name].Starting -= 1
		r.Hosts[name].Mu.Unlock()
		return "", "", err
	}

	if resp.StatusCode() == 429 {
		log.Println("Host is at capacity. Unable to start function instance.")
		r.Hosts[name].Mu.Lock()
		r.Hosts[name].Starting -= 1
		r.Hosts[name].Mu.Unlock()
		return "", "", fmt.Errorf("host is at capacity")
	}

	log.Println(resp.StatusCode())
	resp_body := resp.Body()

	b := struct {
		Ip  string `json:"Ip"`
		Cid string `json:"Cid"`
	}{}
	err = json.Unmarshal(resp_body, &b)
	if err != nil {
		log.Println("Unable to unmarshal")
		r.Hosts[name].Mu.Lock()
		r.Hosts[name].Starting -= 1
		r.Hosts[name].Mu.Unlock()
		return "", "", err
	}

	r.Hosts[name].Mu.Lock()
	r.Hosts[name].Starting -= 1
	r.Hosts[name].Mu.Unlock()
	// use spawned function to run request (cold-start) (forward to newly created function container)
	return b.Ip, b.Cid, nil

}

func (r *RProxy) startNextNode() {
	// TODO: write starting event into log file
	if r.Cluster.Starting {
		log.Println("Already adding node.")
		return
	}
	cmd := exec.Command("./start_node.sh")
	out, err := cmd.Output()
	if err != nil {
		log.Print(err)
	}
	r.Cluster.Mu.Lock()
	r.Cluster.Starting = true
	r.Cluster.Mu.Unlock()
	log.Println(string(out))
}

func (r *RProxy) stopNextNode() {
	// TODO: write stopping event into log file
	cmd := exec.Command("./stop_node.sh")
	out, err := cmd.Output()
	if err != nil {
		log.Print(err)
		return
	}
	addr := string(out)
	log.Println(addr)
	go r.WriteClusterLog(fmt.Sprintf("Stopping node %s", addr))
	delete(r.Cluster.Nodes, addr)
}

func planRessources(nodes []*ClusterNode) (bool, bool) {
	MASTER_CPU_THRESHOLD := 70.0
	WORKER_CPU_THRESHOLD := 70.0
	RAM_THRESHOLD := 90.0
	cpu_avg := 0.0
	ram_avg := 0.0
	cpu_avg_threshold := 0.0
	avg_ram_minus_threshold := 0.0
	avg_cpu_minus_threshold := 0.0
	for i, node := range nodes {
		cpu_avg += node.CpuUsage
		ram_avg += node.RamUsage
		if node.IsMaster {
			cpu_avg_threshold += MASTER_CPU_THRESHOLD
		} else {
			cpu_avg_threshold += WORKER_CPU_THRESHOLD
		}
		if i < len(nodes)-1 {
			avg_ram_minus_threshold += RAM_THRESHOLD
			if node.IsMaster {
				avg_cpu_minus_threshold += MASTER_CPU_THRESHOLD
			} else {
				avg_cpu_minus_threshold += WORKER_CPU_THRESHOLD
			}
		}
	}
	cpu_avg = cpu_avg / float64(len(nodes))
	ram_avg = ram_avg / float64(len(nodes))
	avg_ram_minus_threshold = avg_ram_minus_threshold / float64(len(nodes)-1.0)
	avg_cpu_minus_threshold = avg_cpu_minus_threshold / float64(len(nodes)-1.0)
	cpu_avg_threshold = cpu_avg_threshold / float64(len(nodes))
	return cpu_avg >= cpu_avg_threshold || ram_avg >= RAM_THRESHOLD, cpu_avg < avg_cpu_minus_threshold && ram_avg < avg_ram_minus_threshold
}

func checkNode(node *ClusterNode) bool {
	MASTER_THRESHOLD := 70.0
	WORKER_THRESHOLD := 70.0
	RAM_THRESHOLD := 90.0
	if node.IsMaster {
		return node.CpuUsage < MASTER_THRESHOLD && node.RamUsage <= RAM_THRESHOLD
	} else {
		return node.CpuUsage < WORKER_THRESHOLD && node.RamUsage <= RAM_THRESHOLD
	}
}

func (r *RProxy) WriteUsageLog(text string) {
	ts := time.Now().Format("20060102150405")
	r.Cluster.UsageMu.Lock()
	defer r.Cluster.UsageMu.Unlock()
	f, err := os.OpenFile("usage.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	if _, err = f.WriteString(fmt.Sprintf("%s \t %s\n", ts, text)); err != nil {
		log.Fatalln(err)
	}
}

func (r *RProxy) WriteClusterLog(text string) {
	ts := time.Now().Format("15:04:05.123456")
	r.Cluster.UsageMu.Lock()
	defer r.Cluster.UsageMu.Unlock()
	f, err := os.OpenFile("cluster_scheduling.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	if _, err = f.WriteString(fmt.Sprintf("%s %s\n", ts, text)); err != nil {
		log.Fatalln(err)
	}
}

func (r *RProxy) PlanningLoop() {
	for {
		r.Cluster.Mu.Lock()
		nodes := make([]*ClusterNode, 0)
		for _, node := range r.Cluster.Nodes {
			if node != nil && node.Up {
				nodes = append(nodes, node)
			}
		}
		start, stop := planRessources(nodes)
		r.Cluster.Mu.Unlock()
		now := time.Now()
		if start {
			r.startNextNode()
		} else if stop && !r.Cluster.Starting && r.Cluster.LastJoin.Before(now.Add(-30*time.Second)) {
			r.stopNextNode()
		}

		time.Sleep(2 * time.Second)
	}
}

func (r *RProxy) decideBestRunningLocation() *ClusterNode {
	nodes := make([]*ClusterNode, 0)
	for _, node := range r.Cluster.Nodes {
		if node != nil && node.Up {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { // sort by cpu usage descending
		return nodes[i].CpuUsage > nodes[j].CpuUsage
	})
	for i := 0; i < len(nodes); i++ { // use node with highest load, that is not already above threshold
		if checkNode(nodes[i]) {
			return nodes[i]
		}
	}
	return nil // indicate no available nodes
}

func (r *RProxy) forwardCall(name string, payload []byte, async bool, headers map[string]string, chosenNode *ClusterNode) (Status, []byte) {
	me := r.Cluster.Nodes[r.Cluster.Ip]
	log.Printf("Forwarding call to %s", chosenNode.Address)
	req := &fasthttp.Request{}
	req.SetRequestURI(fmt.Sprintf("http://%s:8000/%s", chosenNode.Address, name))
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
	req.SetBodyRaw(payload)

	resp := &fasthttp.Response{}

	err := r.c.DoTimeout(req, resp, 20*time.Second)

	if err != nil {
		log.Println(err)
		if me.Address != chosenNode.Address {
			log.Println("Node unreachable, marking node as Down.")
			chosenNode.Up = false
		}
		time.Sleep(200 * time.Millisecond)
		return r.fastCall(name, payload, async, headers)
	}

	statusCode := resp.StatusCode()
	respBody := resp.Body()

	if statusCode != http.StatusOK {
		log.Printf("handler returned status code: %d", statusCode)

		return StatusError, nil
	}
	chosenNode.LastUsed = time.Now()
	r.Cluster.Nodes[r.Cluster.Ip] = chosenNode
	return StatusOK, respBody
}

func (r *RProxy) fastCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	me := *r.Cluster.Nodes[r.Cluster.Ip]
	host, ok := r.Hosts[name]
	if !ok {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	var freeCid string
	host.Mu.RLock()
	for cid := range host.Instances {
		if !host.Instances[cid].InUse {
			h := host.Instances[cid]
			h.Mu.Lock()
			h.InUse = true
			h.Mu.Unlock()
			freeCid = cid
			break
		}
	}
	host.Mu.RUnlock()
	if freeCid == "" {
		var chosenNode *ClusterNode = &me
		if me.IsMaster {
			chosenNode = r.decideBestRunningLocation()
			if chosenNode == nil {
				log.Println("Resource scarcity. Unable to schedule.")
				return StatusTooMany, nil
			}
			if chosenNode.Address != me.Address {
				return r.forwardCall(name, payload, async, headers, chosenNode)
			}
		}
		if me.CpuUsage >= 80.0 || me.RamUsage >= 90.0 {
			return StatusTooMany, nil
		}
		go func() {
			ip, cid, err := r.spawnNewInstance(name, headers)
			if err != nil {
				return
			}
			host.Mu.Lock()
			host.Instances[cid] = &FunctionInstance{address: fmt.Sprintf("http://%s:8000/fn", ip), Cid: cid, InUse: false, Mu: &sync.Mutex{}, LastUsed: time.Now()}
			host.Mu.Unlock()
		}()
		return StatusTooMany, nil
	}

	log.Printf("chosen handler: %s", host.Instances[freeCid].address)

	req := &fasthttp.Request{}

	req.SetRequestURI(host.Instances[freeCid].address)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
	req.SetBodyRaw(payload)

	var resp = &fasthttp.Response{}

	err := r.c.DoTimeout(req, resp, 100*time.Second)

	if err != nil {
		log.Println("Unable to complete request, retrying ...")
		log.Println(err)
		return r.fastCall(name, payload, async, headers)
	}

	statusCode := resp.StatusCode()
	respBody := resp.Body()

	if statusCode != http.StatusOK {
		log.Printf("handler returned status code: %d", statusCode)
		return StatusError, nil
	}

	host.Mu.RLock()
	tmp := host.Instances[freeCid]
	host.Mu.RUnlock()
	tmp.Mu.Lock()
	tmp.InUse = false
	tmp.LastUsed = time.Now()
	host.Mu.Lock()
	host.Instances[freeCid] = tmp
	tmp.Mu.Unlock()
	host.Mu.Unlock()
	return StatusOK, respBody
}

func (r *RProxy) normalCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	if len(r.Hosts[name].Instances) > 0 {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)
	var h *FunctionInstance
	for cid, fi := range r.Hosts[name].Instances {
		tmpMu := r.Hosts[name].Instances[cid].Mu
		if !r.Hosts[name].Instances[cid].InUse && tmpMu.TryLock() {
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
