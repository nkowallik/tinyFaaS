package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime/pprof"
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
		log.Println("After Add")
		if err != nil {
			log.Fatalln("Error Adding Function", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Println("Responding")
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
				for name, fis := range r.Hosts {
					for cid, fi := range fis.Instances {
						if r.Hosts[name].Instances[cid].LastUsed.IsZero() {
							log.Println("LastUsed is empty -- continuing")
							continue
						}
						log.Printf("InUse: %t, Age: %f -- (%d)", fi.InUse, time.Since(fi.LastUsed).Seconds(), len(fis.Instances))
						if r.Hosts[name].Instances[cid].InUse || time.Since(r.Hosts[name].Instances[cid].LastUsed).Seconds() < 30.0 { // TODO: make this keep-alive configurable
							continue
						}
						if !r.Hosts[name].Instances[cid].Mu.TryLock() {
							continue
						}
						defer r.Hosts[name].Instances[cid].Mu.Unlock()
						log.Printf("Removing container %s", fi.Cid)
						client := &http.Client{}
						t := struct {
							Cid string
						}{
							Cid: fi.Cid,
						}

						jsonStr, err := json.Marshal(t)
						if err != nil {
							log.Print(err)
							continue
						}
						go func() {
							counter := 0
							for {
								counter++
								req, err := http.NewRequest("POST", "http://127.0.0.1:8080/rminstance", bytes.NewBuffer(jsonStr))
								if err != nil {
									log.Print(err)
									continue
								}
								resp, err := client.Do(req)
								if err != nil {
									log.Print(err)
									continue
								}
								log.Println(resp)
								if resp.StatusCode < 400 {
									r.Hl.Lock()
									delete(r.Hosts[name].Instances, fi.Cid)
									r.Hl.Unlock()
									break
								}
								if counter > 4 {
									log.Println("Stop trying to delete")
									break
								}
							}
						}()
					}
				}
			case <-quit:
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

	s := make(chan os.Signal, 1)

	signal.Notify(s, os.Interrupt)

	<-s

	log.Printf("exiting")
}

func systemWatcher(r *rproxy.RProxy) {
	for {
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
		r.Cluster.Mu.Lock()
		node := r.Cluster.Nodes[r.Cluster.Ip]
		node.CpuUsage = cpu_usage
		node.RamUsage = ram_usage
		r.Cluster.Nodes[r.Cluster.Ip] = node
		r.Cluster.Mu.Unlock()
		tmp := "\n"
		for _, node := range r.Cluster.Nodes {
			if node.Up {
				tmp += fmt.Sprintf("%s:\t %f\t %f\n", node.Address, node.CpuUsage, node.RamUsage)
			}
		}
		log.Println(tmp)
		sleepTime := 10 * time.Second
		if r.Cluster.Worker {
			ip := r.Cluster.Master.Address
			msg := util.StatusMessage{
				CpuUsage: r.Cluster.Nodes[ip].CpuUsage,
				RamUsage: r.Cluster.Nodes[ip].RamUsage,
				Running:  r.Cluster.Nodes[ip].Running,
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
			req.Header.Set("X-tinyFaaS-status", r.Cluster.Ip)

			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.Println("Unable to perform request. Retrying.")
				sleepTime = 100 * time.Millisecond
				continue
			}
			log.Printf("Requested: http://%s:8000", ip)
			log.Printf("Got response: %d", resp.StatusCode)
		}
		time.Sleep(sleepTime) // TODO: adapt sleep time
	}
}
