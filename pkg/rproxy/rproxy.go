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
	"slices"
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

type SafeClusterNodeMap struct {
	values map[string]*ClusterNode
	mu     sync.Mutex
}

func (m *SafeClusterNodeMap) Put(key string, value *ClusterNode) {
	m.mu.Lock()
	m.values[key] = value
	m.mu.Unlock()
}

func (m *SafeClusterNodeMap) Get(key string) *ClusterNode {
	m.mu.Lock()
	me, ok := m.values[key]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return me
}

func (m *SafeClusterNodeMap) Delete(key string) {
	log.Printf("DEL %s", key)
	m.mu.Lock()
	delete(m.values, key)
	m.mu.Unlock()
}

func (m *SafeClusterNodeMap) Keys() []string {
	m.mu.Lock()
	keys := make([]string, len(m.values))
	i := 0
	for k := range m.values {
		keys[i] = k
		i++
	}
	m.mu.Unlock()
	return keys
}

type SafeFunctionHandlerMap struct {
	values map[string]*FunctionHandler
	mu     sync.Mutex
}

func (m *SafeFunctionHandlerMap) Put(key string, value *FunctionHandler) {
	m.mu.Lock()
	m.values[key] = value
	m.mu.Unlock()
}

func (m *SafeFunctionHandlerMap) Get(key string) *FunctionHandler {
	m.mu.Lock()
	me, ok := m.values[key]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return me
}

func (m *SafeFunctionHandlerMap) Delete(key string) {
	m.mu.Lock()
	delete(m.values, key)
	m.mu.Unlock()
}

func (m *SafeFunctionHandlerMap) Keys() []string {
	m.mu.Lock()
	keys := make([]string, len(m.values))
	i := 0
	for k := range m.values {
		keys[i] = k
		i++
	}
	m.mu.Unlock()
	return keys
}

type SafeFunctionInstanceMap struct {
	values map[string]*FunctionInstance
	mu     sync.Mutex
}

func (m *SafeFunctionInstanceMap) Put(key string, value *FunctionInstance) {
	m.mu.Lock()
	m.values[key] = value
	m.mu.Unlock()
}

func (m *SafeFunctionInstanceMap) Get(key string) *FunctionInstance {
	m.mu.Lock()
	me, ok := m.values[key]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return me
}

func (m *SafeFunctionInstanceMap) Delete(key string) {
	m.mu.Lock()
	delete(m.values, key)
	m.mu.Unlock()
}

func (m *SafeFunctionInstanceMap) Keys() []string {
	m.mu.Lock()
	keys := make([]string, len(m.values))
	i := 0
	for k := range m.values {
		keys[i] = k
		i++
	}
	m.mu.Unlock()
	return keys
}

type Cluster struct {
	Nodes       *SafeClusterNodeMap
	Mu          *sync.RWMutex
	Ip          string
	Worker      *SafeBool
	Master      *ClusterNode
	Starting    *SafeBool
	LastJoin    time.Time
	UsageMu     *sync.Mutex
	LastStop    time.Time
	StartNodeMu *sync.Mutex
}

type SafeInt struct {
	mu    sync.Mutex
	value int
}

func (i *SafeInt) Get() int {
	i.mu.Lock()
	me := i.value
	i.mu.Unlock()
	return me
}

func (i *SafeInt) Increase() {
	i.mu.Lock()
	i.value += 1
	i.mu.Unlock()
}

func (i *SafeInt) Decrease() {
	i.mu.Lock()
	i.value -= 1
	i.mu.Unlock()
}

func (i *SafeInt) Set(num int) {
	i.mu.Lock()
	i.value = num
	i.mu.Unlock()
}

type SafeBool struct {
	mu    sync.Mutex
	value bool
}

func (b *SafeBool) Get() bool {
	b.mu.Lock()
	me := b.value
	b.mu.Unlock()
	return me
}

func (b *SafeBool) Set(val bool) {
	b.mu.Lock()
	b.value = val
	b.mu.Unlock()
}

type SafeTime struct {
	value time.Time
	mu    sync.Mutex
}

func (t *SafeTime) Get() time.Time {
	t.mu.Lock()
	me := t.value
	t.mu.Unlock()
	return me
}

func (t *SafeTime) Set(tmp time.Time) {
	t.mu.Lock()
	t.value = tmp
	t.mu.Unlock()
}

type ClusterNode struct {
	Address         string    `json:"address"`
	CpuUsage        float64   `json:"cpuusage"`
	RamUsage        float64   `json:"ramusage"`
	CpuHistory      []float64 `json:"cpuhistory"`
	RamHistory      []float64 `json:"ramhistory"`
	Running         *SafeInt  `json:"running"`
	InUse           *SafeInt  `json:"used"`
	Up              *SafeBool `json:"up"`
	LastUsed        *SafeTime `json:"lastused"`
	IsMaster        *SafeBool `json:"ismaster"`
	TooManyRequests int64     `json:"toomanyrequests"`
}

func (c *ClusterNode) GetCpu() float64 {
	if len(c.CpuHistory) == 0 {
		return 0
	}

	return slices.Max(c.CpuHistory)
}

func (c *ClusterNode) GetRam() float64 {
	if len(c.RamHistory) == 0 {
		return 0
	}

	return slices.Max(c.RamHistory)
}

type FunctionHandler struct {
	Instances *SafeFunctionInstanceMap
	Mu        *sync.RWMutex
	SpawnMu   *sync.Mutex
	Starting  *SafeInt
}

type FunctionInstance struct {
	address  string
	Cid      string
	InUse    *SafeBool
	LastUsed *SafeTime
	Mu       *sync.Mutex
}

type RProxy struct {
	Hosts        SafeFunctionHandlerMap // TODO: NilPointerDereference
	c            *fasthttp.Client
	Hl           sync.RWMutex
	Cluster      *Cluster
	LastActivity time.Time
	Joining      *SafeBool
	Starting     *SafeBool
}

func New() *RProxy {
	addr := GetOutboundIP().String()
	node := ClusterNode{
		Address:         addr,
		CpuUsage:        -1.0,
		RamUsage:        -1.0,
		Running:         &SafeInt{value: 0, mu: sync.Mutex{}},
		InUse:           &SafeInt{value: 0, mu: sync.Mutex{}},
		CpuHistory:      make([]float64, 0),
		RamHistory:      make([]float64, 0),
		Up:              &SafeBool{value: true, mu: sync.Mutex{}},
		LastUsed:        &SafeTime{value: time.Now(), mu: sync.Mutex{}},
		IsMaster:        &SafeBool{value: true, mu: sync.Mutex{}},
		TooManyRequests: 0,
	}
	nodes := SafeClusterNodeMap{values: make(map[string]*ClusterNode), mu: sync.Mutex{}}
	nodes.Put(addr, &node)
	return &RProxy{
		Hosts: SafeFunctionHandlerMap{values: make(map[string]*FunctionHandler), mu: sync.Mutex{}},
		c: &fasthttp.Client{
			MaxConnsPerHost:        256 * 1024,
			DisablePathNormalizing: true,
			// increase DNS cache time to an hour instead of default minute
			DialTimeout: (&fasthttp.TCPDialer{
				Concurrency: 0,
			}).DialTimeout,
		},
		Cluster: &Cluster{
			Nodes:       &nodes,
			Mu:          &sync.RWMutex{},
			Ip:          addr,
			Worker:      &SafeBool{value: false, mu: sync.Mutex{}},
			Starting:    &SafeBool{value: false, mu: sync.Mutex{}},
			Master:      &node,
			LastJoin:    time.Now(),
			UsageMu:     &sync.Mutex{},
			LastStop:    time.Now(),
			StartNodeMu: &sync.Mutex{},
		},
		Starting:     &SafeBool{value: true, mu: sync.Mutex{}},
		Joining:      &SafeBool{value: false, mu: sync.Mutex{}},
		LastActivity: time.Now(),
	}
}

func (r *RProxy) Add(name string, ips []util.IpWrapper) error {
	r.Hl.Lock()
	defer r.Hl.Unlock()

	r.Hosts.Put(name, &FunctionHandler{Instances: &SafeFunctionInstanceMap{values: make(map[string]*FunctionInstance), mu: sync.Mutex{}}, Mu: &sync.RWMutex{}, SpawnMu: &sync.Mutex{}, Starting: &SafeInt{value: 0, mu: sync.Mutex{}}})

	for i, ip := range ips {
		ips[i] = util.IpWrapper{Ip: fmt.Sprintf("http://%s:8000/fn", ip.Ip), Cid: ip.Cid}
	}
	for _, ip := range ips {
		tmp := r.Hosts.Get(name)
		if tmp != nil {
			tmp.Instances.Put(ip.Cid, &FunctionInstance{address: ip.Ip, Cid: ip.Cid, LastUsed: &SafeTime{value: time.Now(), mu: sync.Mutex{}}, InUse: &SafeBool{value: false, mu: sync.Mutex{}}, Mu: &sync.Mutex{}})
		}
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
	//r.Cluster.Mu.Lock()
	//defer r.Cluster.Mu.Unlock()
	node := r.Cluster.Nodes.Get(nodeAddr)
	//log.Printf("%s \t RAM: %f; CPU: %f", nodeAddr, msg.RamUsage, msg.CpuUsage)
	go r.WriteUsageLog(fmt.Sprintf("%s \t%f;%f", nodeAddr, msg.CpuUsage, msg.RamUsage))
	if node == nil {
		cpuhistory := make([]float64, 1)
		cpuhistory[0] = msg.CpuUsage
		ramhistory := make([]float64, 1)
		ramhistory[0] = msg.RamUsage
		r.Cluster.Nodes.Put(nodeAddr, &ClusterNode{
			Address:         nodeAddr,
			CpuUsage:        msg.CpuUsage,
			CpuHistory:      cpuhistory,
			RamUsage:        msg.RamUsage,
			RamHistory:      ramhistory,
			Running:         &SafeInt{value: msg.Running, mu: sync.Mutex{}},
			InUse:           &SafeInt{value: msg.InUse, mu: sync.Mutex{}},
			Up:              &SafeBool{value: true, mu: sync.Mutex{}},
			LastUsed:        &SafeTime{value: time.Now(), mu: sync.Mutex{}},
			IsMaster:        &SafeBool{value: false, mu: sync.Mutex{}},
			TooManyRequests: 0,
		})
	} else {
		node.CpuUsage = msg.CpuUsage
		node.CpuHistory = append(node.CpuHistory, msg.CpuUsage)
		for len(node.CpuHistory) > 10 {
			node.CpuHistory = node.CpuHistory[1:]
		}
		node.RamUsage = msg.RamUsage
		node.RamHistory = append(node.RamHistory, msg.RamUsage)
		for len(node.RamHistory) > 10 {
			node.RamHistory = node.RamHistory[1:]
		}
		node.Up.Set(true)
		node.Running.Set(msg.Running)
		node.InUse.Set(msg.InUse)
		node.TooManyRequests = msg.TooManyRequests
		r.Cluster.Nodes.Put(nodeAddr, node)
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
	r.Cluster.Nodes.Put(ip, &ClusterNode{
		Address:  ip,
		CpuUsage: msg.CpuUsage,
		RamUsage: msg.RamUsage,
		Running:  &SafeInt{value: 0, mu: sync.Mutex{}},
		InUse:    &SafeInt{value: 0, mu: sync.Mutex{}},
		Up:       &SafeBool{value: true, mu: sync.Mutex{}},
		LastUsed: &SafeTime{value: time.Now(), mu: sync.Mutex{}},
		IsMaster: &SafeBool{value: false, mu: sync.Mutex{}},
	})
	r.Cluster.LastJoin = time.Now()
	r.Cluster.Starting.Set(false)
	return *r.Cluster.Master, nil
}

func (r *RProxy) GetIP() string {
	return GetOutboundIP().String()
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

func (r *RProxy) LeaveCluster(addr string) error {
	log.Printf("%s is leaving the cluster", addr)
	r.Cluster.Nodes.Delete(addr)
	r.stopNextNode(addr)
	return nil
}

func (r *RProxy) RegisterInCluster(addr string) error {
	r.Joining.Set(true)
	log.Printf("Registering in Custer %s", addr)
	outboundIp := r.Cluster.Ip
	me := r.Cluster.Nodes.Get(outboundIp)
	if me == nil {
		return fmt.Errorf("unable to find myself in cluster nodes")
	}
	cpu_usage := me.CpuUsage
	ram_usage := me.RamUsage
	msg := util.StatusMessage{
		CpuUsage: cpu_usage,
		RamUsage: ram_usage,
		Running:  me.Running.Get(),
		InUse:    me.InUse.Get(),
	}
	jsonStr, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
		r.Joining.Set(false)
		return fmt.Errorf("unable to marshal function register")
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/", addr), bytes.NewBuffer(jsonStr))
	if err != nil {
		log.Fatal(err)
		r.Joining.Set(false)
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
	r.Cluster.Master.IsMaster.Set(false)
	r.Cluster.Nodes.Put(r.Cluster.Master.Address, r.Cluster.Master)
	r.Cluster.Nodes.Put(tmp.Address, &tmp)
	r.Cluster.Master = r.Cluster.Nodes.Get(tmp.Address)
	r.Cluster.Worker.Set(true)
	r.Joining.Set(false)
	return nil
}

func (r *RProxy) Del(name string) error {
	r.Hl.Lock()
	defer r.Hl.Unlock()

	if ok := r.Hosts.Get(name); ok == nil {
		return fmt.Errorf("function not found")
	}
	r.Hosts.Delete(name)
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
			me := r.Cluster.Nodes.Get(r.GetIP())
			if me == nil {
				log.Println("ME NIL")
				break
			}
			me.Running.Decrease()
			target := r.Hosts.Get(name)
			if target == nil {
				log.Println("TARGET NIL")
				break
			}
			target.Instances.Delete(cid)
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
	r.LastActivity = time.Now()
	target := r.Hosts.Get(name)
	if target == nil {
		return "", "", fmt.Errorf("unable to find FunctionHandler for name %s", name)
	}
	target.Starting.Increase()
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
		target.Starting.Decrease()
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
		target.Starting.Decrease()
		return "", "", err
	}

	if resp.StatusCode() == 429 {
		log.Println("Host is at capacity. Unable to start function instance.")
		target.Starting.Decrease()
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
		target.Starting.Decrease()
		return "", "", err
	}

	target.Starting.Decrease()
	me := r.Cluster.Nodes.Get(r.GetIP())
	if me == nil {
		return "", "", fmt.Errorf("unable to find %s in cluster nodes", r.GetIP())
	}
	me.Running.Increase()
	// use spawned function to run request (cold-start) (forward to newly created function container)
	return b.Ip, b.Cid, nil

}

func (r *RProxy) startNextNode() {
	if !r.Cluster.Starting.Get() && r.Cluster.StartNodeMu.TryLock() {
		r.Cluster.Starting.Set(true)
	} else {
		log.Println("Already adding node.")
		return
	}
	cmd := exec.Command("./start_node.sh")
	out, err := cmd.Output()
	if err != nil {
		log.Print(err)
	}
	addr := string(out)
	r.WriteClusterLog(fmt.Sprintf("Starting node %s", addr))
	r.Cluster.StartNodeMu.Unlock()
}

func (r *RProxy) stopNextNode(addr string) {
	log.Printf("./stop_node.sh %s", addr)
	cmd := exec.Command(fmt.Sprintf("./stop_node.sh %s", addr))
	_, err := cmd.Output()
	if err != nil {
		log.Print(err)
		return
	}
	log.Println(addr)
	go r.WriteClusterLog(fmt.Sprintf("Stopping node %s", addr))
}

func planRessources(nodes []*ClusterNode) bool {
	for _, node := range nodes {
		if node.InUse.Get() < 19 && int(node.TooManyRequests) < 2 {
			node.TooManyRequests = 0
			return false
		}
		node.TooManyRequests = 0
	}
	return true
	/*MASTER_CPU_THRESHOLD := 70.0
	WORKER_CPU_THRESHOLD := 70.0
	RAM_THRESHOLD := 75.01
	cpu_avg := 0.0
	ram_avg := 0.0
	cpu_avg_threshold := 0.0
	avg_ram_minus_threshold := 0.0
	avg_cpu_minus_threshold := 0.0
	for i, node := range nodes {
		cpu_avg += node.GetCpu()
		ram_avg += node.GetRam()
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
	avg_ram_minus_threshold = avg_ram_minus_threshold / float64(len(nodes)-1)
	avg_cpu_minus_threshold = avg_cpu_minus_threshold / float64(len(nodes)-1)
	cpu_avg_threshold = cpu_avg_threshold / float64(len(nodes))
	log.Printf("CPU: %f; RAM: %f; CPUT: %f; RAMT: %f", cpu_avg, ram_avg, avg_cpu_minus_threshold, avg_ram_minus_threshold)
	return cpu_avg >= cpu_avg_threshold || ram_avg >= RAM_THRESHOLD, cpu_avg < avg_cpu_minus_threshold && ram_avg < avg_ram_minus_threshold*/
}

func checkNode(node *ClusterNode) bool {
	//WORKER_THRESHOLD := 80.0
	//RAM_THRESHOLD := 80.0
	//return node.GetCpu() < WORKER_THRESHOLD && node.GetRam() <= RAM_THRESHOLD
	log.Printf("%s: %d/%d", node.Address, node.InUse.Get(), node.Running.Get())
	return node.InUse.Get() < node.Running.Get() || node.Running.Get() < 19
}

func (r *RProxy) WriteUsageLog(text string) {
	ts := time.Now().Format("20060102150405")
	r.Cluster.UsageMu.Lock()
	defer r.Cluster.UsageMu.Unlock()
	f, err := os.OpenFile("/home/pi/usage.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
	f, err := os.OpenFile("/home/pi/cluster_scheduling.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
		nodes := make([]*ClusterNode, 0)
		for _, key := range r.Cluster.Nodes.Keys() {
			node := r.Cluster.Nodes.Get(key)
			if node != nil && node.Up.Get() {
				nodes = append(nodes, node)
			}
		}

		if planRessources(nodes) {
			r.startNextNode()
		}
		/* else if stop && !r.Cluster.Starting && time.Now().After(r.Cluster.LastJoin.Add(20*time.Second)) {
			r.stopNextNode()
		}*/

		time.Sleep(5 * time.Second)
	}
}

func (r *RProxy) decideBestRunningLocation() *ClusterNode {
	nodes := make([]*ClusterNode, 0)
	for _, key := range r.Cluster.Nodes.Keys() {
		node := r.Cluster.Nodes.Get(key)
		if node != nil && node.Up.Get() {
			nodes = append(nodes, node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { // sort by cpu usage descending
		return nodes[i].InUse.Get() > nodes[j].InUse.Get()
	})
	for i := 0; i < len(nodes); i++ { // use node with highest load, that is not already above threshold
		if checkNode(nodes[i]) {
			return nodes[i]
		}
	}
	return nil // indicate no available nodes
}

func (r *RProxy) forwardCall(name string, payload []byte, chosenNode *ClusterNode) (Status, []byte) {
	me := r.Cluster.Nodes.Get(r.GetIP())
	log.Printf("Forwarding call to %s", chosenNode.Address)
	chosenNode.LastUsed.Set(time.Now())
	go func() {
		req := &fasthttp.Request{}
		req.SetRequestURI(fmt.Sprintf("http://%s:8000/%s", chosenNode.Address, name))
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
		req.SetBodyRaw(payload)

		resp := &fasthttp.Response{}

		err := r.c.DoTimeout(req, resp, 20*time.Second)

		if err != nil {
			log.Println("FORWARD ERROR")
			log.Println(err)
			if me.Address != chosenNode.Address {
				log.Println("Node unreachable, marking node as Down.")
				chosenNode.Up.Set(false)
			}
			return
		}

		statusCode := resp.StatusCode()

		log.Printf("handler returned status code: %d", statusCode)
		chosenNode.LastUsed.Set(time.Now())
		//r.Cluster.Nodes.Put(r.Cluster.Ip, chosenNode)
	}()

	return StatusOK, []byte("Ok")
}

func (r *RProxy) fastCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	r.LastActivity = time.Now()
	myIp := r.GetIP()
	tmp := r.Cluster.Nodes.Get(myIp)
	if tmp == nil {
		log.Printf("%s not in cluster nodes", myIp)
		return StatusNotFound, nil
	}
	me := *tmp
	host := r.Hosts.Get(name)
	if host == nil {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	var freeCid string
	host.Mu.RLock()
	for _, cid := range host.Instances.Keys() {
		if host.Instances.Get(cid).Mu.TryLock() {
			host.Instances.Get(cid).InUse.Set(true)
			freeCid = cid
		}
	}
	host.Mu.RUnlock()
	if freeCid == "" {
		if me.Running.Get() < 19 {
			ip, cid, err := r.spawnNewInstance(name, headers)
			if err != nil {
				return StatusError, nil
			}
			host.Instances.Put(cid, &FunctionInstance{address: fmt.Sprintf("http://%s:8000/fn", ip), Cid: cid, InUse: &SafeBool{value: false, mu: sync.Mutex{}}, Mu: &sync.Mutex{}, LastUsed: &SafeTime{value: time.Now(), mu: sync.Mutex{}}})
			freeCid = cid
		}
		var chosenNode *ClusterNode = &me
		if freeCid == "" && me.IsMaster.Get() {
			chosenNode = r.decideBestRunningLocation()
			if chosenNode == nil {
				go r.startNextNode()
				log.Println("Resource scarcity. Unable to schedule. Scheduling New Instance.")
				return StatusTooMany, nil
			}
			log.Printf("%s VS. %s", chosenNode.Address, me.Address)
			if chosenNode.Address != me.Address {
				return r.forwardCall(name, payload, chosenNode)
			}
		}

		me.TooManyRequests += 1
		return StatusTooMany, nil
	}
	me.LastUsed.Set(time.Now())
	instance := host.Instances.Get(freeCid)
	if instance == nil {
		return StatusError, nil
	}
	log.Printf("chosen handler: %s", instance.address)
	go func(FnsInUse *SafeInt, InUse *SafeBool, mu *sync.Mutex, t *SafeTime) {
		FnsInUse.Increase()
		req := &fasthttp.Request{}
		instance.LastUsed.Set(time.Now())
		req.SetRequestURI(instance.address)
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentTypeBytes([]byte("application/octet-stream"))
		req.SetBodyRaw(payload)

		var resp = &fasthttp.Response{}

		err := r.c.DoTimeout(req, resp, 100*time.Second)

		if err != nil {
			log.Println("Unable to complete request.")
			log.Println(err)
			mu.Unlock()
			FnsInUse.Decrease()
			InUse.Set(false)
			t.Set(time.Now())
			return
		}
		mu.Unlock()
		FnsInUse.Decrease()
		InUse.Set(false)
		t.Set(time.Now())
		instance.LastUsed.Set(time.Now())
	}(me.InUse, instance.InUse, instance.Mu, tmp.LastUsed)

	return StatusOK, []byte("Ok")
}

func (r *RProxy) normalCall(name string, payload []byte, async bool, headers map[string]string) (Status, []byte) {
	if len(r.Hosts.Get(name).Instances.Keys()) > 0 {
		log.Printf("function not found: %s", name)
		return StatusNotFound, nil
	}

	// log.Printf("have handlers: %s", handler)
	var h *FunctionInstance
	for _, cid := range r.Hosts.Get(name).Instances.Keys() {
		fi := r.Hosts.Get(name).Instances.Get(cid)
		tmpMu := fi.Mu
		if !fi.InUse.Get() && tmpMu.TryLock() {
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
