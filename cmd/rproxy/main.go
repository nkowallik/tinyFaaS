package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/coap"
	"github.com/OpenFogStack/tinyFaaS/pkg/fastcoap"
	"github.com/OpenFogStack/tinyFaaS/pkg/grpc"
	tfhttp "github.com/OpenFogStack/tinyFaaS/pkg/http"
	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
)

func main() {
	// add cpu profile
	f, e := os.Create("cpu.prof")

	if e != nil {
		log.Fatalf("could not create cpu profile: %s", e)
	}

	defer f.Close()

	e = pprof.StartCPUProfile(f)

	if e != nil {
		log.Fatalf("could not start cpu profile: %s", e)
	}

	defer pprof.StopCPUProfile()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("rproxy: ")

	if len(os.Args) <= 3 {
		fmt.Println("Usage: ./rproxy <listen-addr> [<protocol>:<listen-addr>]")
		os.Exit(1)
	}

	f, err := os.OpenFile("rproxy.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error creating logfile rproxy.log: %v", err)
	}
	defer f.Close()
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)

	rproxyListenAddress := os.Args[1]

	listenAddrs := make(map[string]string)

	for _, arg := range os.Args[2:] {
		prot, listenAddr, ok := strings.Cut(arg, ":")

		if !ok {
			fmt.Println("Usage: ./rproxy <listen-addr> <protocol>:<listen-addr>")
			os.Exit(1)
		}

		prot = strings.ToLower(prot)
		listenAddr = strings.ToLower(listenAddr)

		log.Printf("adding %s listener on %s", prot, listenAddr)
		listenAddrs[prot] = listenAddr
	}

	if len(listenAddrs) == 0 {
		return // nothing to do
	}

	r := rproxy.New()
	go func() {
		time.Sleep(1 * time.Minute)
		r.Starting.Set(false)
		r.LastActivity.Set(time.Now())
	}()
	// CoAP
	if listenAddr, ok := listenAddrs["coap"]; ok {
		log.Printf("starting coap server on %s", listenAddr)
		go coap.Start(r, listenAddr)
	}

	// Fast CoAP
	if listenAddr, ok := listenAddrs["fastcoap"]; ok {
		log.Printf("starting coap server on %s", listenAddr)
		go fastcoap.Start(r, listenAddr)
	}

	// HTTP
	if listenAddr, ok := listenAddrs["http"]; ok {
		log.Printf("starting http server on %s", listenAddr)
		go tfhttp.Start(r, listenAddr)
	}
	// GRPC
	if listenAddr, ok := listenAddrs["grpc"]; ok {
		log.Printf("starting grpc server on %s", listenAddr)
		go grpc.Start(r, listenAddr)
	}
	// FastHTTP
	if listenAddr, ok := listenAddrs["fasthttp"]; ok {
		log.Printf("starting fasthttp server on %s", listenAddr)
		go tfhttp.StartFastHTTP(r, listenAddr)
	}

	server := http.NewServeMux()

	server.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		log.Printf("have request: %+v", req)

		buf := new(bytes.Buffer)
		buf.ReadFrom(req.Body)
		newStr := buf.String()

		log.Printf("have body: %s", newStr)

		var def struct {
			FunctionResource   string           `json:"name"`
			FunctionContainers []util.IpWrapper `json:"ips"`
		}

		err := json.Unmarshal([]byte(newStr), &def)

		if err != nil {
			log.Fatalln("Error Unmarshalling", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Printf("have definition: %+v", def)

		if def.FunctionResource[0] == '/' {
			def.FunctionResource = def.FunctionResource[1:]
		}

		log.Printf("adding %s", def.FunctionResource)
		err = r.Add(def.FunctionResource, def.FunctionContainers)
		if err != nil {
			log.Fatalln("Error Adding Function", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				for _, name := range r.Hosts.Keys() {
					fis := r.Hosts.Get(name)
					for _, cid := range fis.Instances.Keys() {
						fi := fis.Instances.Get(cid)
						if fi.LastUsed.Get().IsZero() {
							log.Println("LastUsed is empty -- continuing")
							continue
						}
						//log.Printf("InUse: %t, Age: %f -- (%d)", fi.InUse.Get(), time.Since(fi.LastUsed.Get()).Seconds(), len(fis.Instances.Keys()))
						/*if fi.InUse.Get() {
							go func(fi *rproxy.FunctionInstance) {
								target := strings.Replace(fi.Address, "/fn", "/inuse", 1)
								req, err := http.NewRequest("GET", target, bytes.NewBufferString(""))
								if err != nil {
									log.Println(err)
								}

								client := http.Client{}
								resp, err := client.Do(req)
								if err != nil {
									log.Println("Unable to perform request.")
									return
								}
								log.Printf("Got response: %d", resp.StatusCode)
								buf := new(bytes.Buffer)
								buf.ReadFrom(resp.Body)
								response_body := buf.String()
								log.Printf("RESPONSE: %s", response_body)
							}(fi)
						}*/
						if fi.InUse.Get() || time.Since(fi.LastUsed.Get()).Seconds() < 15.0 { // TODO: make this keep-alive configurable
							continue
						}
						if !fi.Mu.TryLock() {
							continue
						}
						defer fi.Mu.Unlock()
						log.Printf("Removing container %s", fi.Cid.Get())
						err = r.StopInstance(cid, name)
						if err != nil {
							log.Println("Unable to stop instance!")
							go r.StopInstance(cid, name)
							continue
						}
					}
				}
			case <-quit:
				log.Println("STOPPING STOP_LOOP")
				ticker.Stop()
				return
			}
		}
	}()
	/*alerter := time.NewTicker(5 * time.Second)
	alerter_quit := make(chan struct{})
	go func(alerter *time.Ticker) {
		log.Println("Tick")
		select {
		case <-alerter.C:
			log.Println("Selected")
			if r.Cluster.Worker {
				log.Println("Worker")
				ip := r.Cluster.Master.Address
				msg := struct {
					CpuUsage float64          `json:"cpuUsage"`
					Running  map[string]int16 `json:"running"`
				}{
					CpuUsage: r.Cluster.Nodes[ip].CpuUsage,
					Running:  r.Cluster.Nodes[ip].Running,
				}
				jsonStr, err := json.Marshal(msg)
				if err != nil {
					log.Println("Unable to marshal cpu message")
					log.Panic(err)
				}
				req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8080", ip), bytes.NewBuffer(jsonStr))
				if err != nil {
					log.Fatal(err)
				}
				req.Header.Set("X-tinyFaaS-status", r.Cluster.Ip)
				req.Header.Set("Content-Type", "application/json")

				client := http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Println("Unable to perform request")
					panic(err)
				}
				log.Printf("Got response: %d", resp.StatusCode)
			}
		case <-alerter_quit:
			alerter.Stop()
			return
		}
	}(alerter)
	*/
	go func() {
		log.Printf("listening on %s", rproxyListenAddress)
		err := http.ListenAndServe(rproxyListenAddress, server)

		if err != nil {
			log.Printf("%s", err)
		}
	}()
	go systemWatcher(r)
	go r.PlanningLoop()
	go clusterStatusWatcher(r)
	//go clusterWatcher(r)

	s := make(chan os.Signal, 1)

	signal.Notify(s, os.Interrupt)

	<-s

	log.Printf("exiting")
}

func clusterStatusWatcher(r *rproxy.RProxy) {
	for {
		if r == nil || r.Cluster == nil || r.Cluster.Nodes == nil || len(r.Cluster.Nodes.Keys()) == 0 {
			break
		}
		nodes := make([]*rproxy.ClusterNode, len(r.Cluster.Nodes.Keys()))
		for i, key := range r.Cluster.Nodes.Keys() {
			nodes[i] = r.Cluster.Nodes.Get(key)
		}

		sort.Slice(nodes, func(i, j int) bool { // sort by cpu usage descending
			return nodes[i].InUse.Get() > nodes[j].InUse.Get()
		})

		for _, node := range nodes {
			log.Printf("%s: \t%d / %d \t(%s)", node.Address.Get(), node.InUse.Get(), node.Running.Get(), r.Cluster.Starting.Get())
			go r.WriteInstanceLog(fmt.Sprintf("%s: \t%d / %d", node.Address.Get(), node.InUse.Get(), node.Running.Get()))
		}

		time.Sleep(1 * time.Second)
	}
}

/*func clusterWatcher(r *rproxy.RProxy) {
	for {
		if len(r.Cluster.Nodes.Keys()) < 1 {
			time.Sleep(10 * time.Second)
			continue
		}
		if r.Cluster.Master.Address != r.Cluster.Ip {
			log.Println("EXITING LOOP")
			break
		}
		now := time.Now()
		cutoff := now.Add(-30 * time.Second)
		for _, key := range r.Cluster.Nodes.Keys() {
			node := r.Cluster.Nodes.Get(key)
			if node.LastUsed.Get().Before(cutoff) && !node.IsMaster.Get() {
				log.Println(node)
				log.Printf("Cluster Watcher is killing %s", key)
				r.Cluster.Nodes.Delete(key)
			}
		}

		time.Sleep(10 * time.Second)
	}
}*/

func systemWatcher(r *rproxy.RProxy) {
	for {
		if r == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		tip := r.GetIP()
		me := r.Cluster.Nodes.Get(tip)
		if me == nil {
			log.Printf("%s not in cluster", tip)
			log.Println(r.Cluster.Nodes)
			time.Sleep(1 * time.Second)
			continue
		}
		ip := r.Cluster.Master.Address.Get()
		if _, err := os.Stat("./alwayson"); err == nil {
			log.Println("Skipping leave cluster check")
		} else if errors.Is(err, os.ErrNotExist) {
			if !me.IsMaster.Get() && !r.Starting.Get() && !r.Joining.Get() && r.LastActivity.Get().Add(30*time.Second).Before(time.Now()) && me.InUse.Get() == 0 {
				req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000", ip), bytes.NewBufferString(""))
				if err != nil {
					log.Println(err)
				}
				req.Header.Set("X-tinyFaaS-leavecluster", me.Address.Get())

				client := http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					log.Println("Unable to perform request.")
					continue
				}
				log.Printf("Requested Cluster Leave: http://%s:8000", ip)
				log.Printf("Got response: %d", resp.StatusCode)
				if resp.StatusCode < 400 {
					break
				}
			}
		}

		cmd := exec.Command("./get_cpu_usage.sh")
		out, err := cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		cpu_usage, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 32)
		if err != nil {
			log.Fatal(err)
		}
		cmd2 := exec.Command("./get_ram_usage.sh")
		out2, err := cmd2.Output()
		if err != nil {
			log.Fatal(err)
		}
		ram_usage, err := strconv.ParseFloat(strings.TrimSpace(string(out2)), 32)
		if err != nil {
			log.Fatal(err)
		}
		go r.WriteUsageLog(fmt.Sprintf("%s \t%f;%f", r.Cluster.Ip.Get(), cpu_usage, ram_usage))
		r.Cluster.Mu.Lock()
		node := r.Cluster.Nodes.Get(r.Cluster.Ip.Get())
		if node == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		node.CpuUsage = cpu_usage
		node.CpuHistory = append(node.CpuHistory, cpu_usage)
		for len(node.CpuHistory) > 10 {
			node.CpuHistory = node.CpuHistory[1:]
		}
		node.RamUsage = ram_usage
		node.RamHistory = append(node.RamHistory, ram_usage)
		for len(node.RamHistory) > 10 {
			node.RamHistory = node.RamHistory[1:]
		}
		r.Cluster.Nodes.Put(r.Cluster.Ip.Get(), node)
		r.Cluster.Mu.Unlock()
		/*tmp := "\n"
		for _, node := range r.Cluster.Nodes {
			if node.Up {
				tmp += fmt.Sprintf("%s:\t %f\t %f\n", node.Address, node.InUse.Get(), node.Running.Get())
			}
		}
		log.Println(tmp)*/
		// TODO: create log file containing timestamp, address, cpu and ram usage for each node
		sleepTime := 500 * time.Millisecond
		if r.Cluster.Worker.Get() {
			log.Println("Sending Update To Leader")
			msg := util.StatusMessage{
				CpuUsage:        cpu_usage,
				RamUsage:        ram_usage,
				Running:         me.Running.Get(),
				InUse:           me.InUse.Get(),
				TooManyRequests: me.TooManyRequests.Get(),
			}
			jsonStr, err := json.Marshal(msg)
			if err != nil {
				log.Println("Unable to marshal cpu message")
				log.Println(err)
			}
			req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000", ip), bytes.NewBuffer(jsonStr))
			if err != nil {
				log.Println(err)
			}
			req.Header.Set("X-tinyFaaS-status", r.Cluster.Ip.Get())
			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.Println("Unable to perform request. Retrying.")
				sleepTime = 100 * time.Millisecond
				continue
			}
			log.Printf("Requested: http://%s:8000", ip)
			log.Printf("Got response: %d", resp.StatusCode)
			if resp.StatusCode < 400 {
				me.TooManyRequests.Set(0)
			}
		}
		time.Sleep(sleepTime)
	}
}
