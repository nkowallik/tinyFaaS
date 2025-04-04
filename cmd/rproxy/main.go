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
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Printf("have definition: %+v", def)

		if def.FunctionResource[0] == '/' {
			def.FunctionResource = def.FunctionResource[1:]
		}

		//if len(def.FunctionContainers) > 0 {
		// "ips" field not empty: add function
		log.Printf("adding %s", def.FunctionResource)
		err = r.Add(def.FunctionResource, def.FunctionContainers)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return
		/*} else {

			log.Printf("deleting %s", def.FunctionResource)
			err = r.Del(def.FunctionResource)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return

			}
		}*/
	})
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				for name, fis := range r.Hosts {
					if len(fis) > 0 {
						log.Println(name)
					}
					for cid, fi := range fis {
						if r.Hosts[name][cid].LastUsed.IsZero() {
							log.Println("LastUsed is empty -- continuing")
							continue
						}
						log.Printf("InUse: %t, Age: %f -- (%d)", fi.InUse, time.Since(fi.LastUsed).Seconds(), len(fis))
						if r.Hosts[name][cid].InUse || time.Since(r.Hosts[name][cid].LastUsed).Seconds() < 30.0 { // TODO: make this keep-alive configurable
							continue
						}
						if !r.Hosts[name][cid].Mu.TryLock() {
							continue
						}
						defer r.Hosts[name][cid].Mu.Unlock()
						log.Printf("Removing container %s", fi.Cid)
						r.Hl.Lock()
						delete(r.Hosts[name], fi.Cid)
						r.Hl.Unlock()
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

						req, err := http.NewRequest("POST", "http://127.0.0.1:8080/rminstance", bytes.NewBuffer(jsonStr))
						if err != nil {
							log.Print(err)
							continue
						}
						resp, err := client.Do(req)
						if err != nil {
							log.Print(err)
						}
						log.Println(resp)
					}
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	// TODO: this should probably only be started after joining a cluster
	alerter := time.NewTicker(5 * time.Second)
	go func() {
		select {
		case <-alerter.C:
			// TODO: send update to cluster leader, if part of a cluster
		case <-quit:
			alerter.Stop()
			return
		}
	}()

	go func() {
		log.Printf("listening on %s", rproxyListenAddress)
		err := http.ListenAndServe(rproxyListenAddress, server)

		if err != nil {
			log.Printf("%s", err)
		}
	}()
	go cpuWatcher(r)

	s := make(chan os.Signal, 1)

	signal.Notify(s, os.Interrupt)

	<-s

	log.Printf("exiting")
}

func cpuWatcher(r *rproxy.RProxy) {
	var count = 0
	for {
		cmd := exec.Command("./get_cpu_usage.sh")
		out, err := cmd.Output()
		if err != nil {
			log.Fatal(err)
		}
		usage, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 32)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Usage: %f", usage)
		r.Cluster.Mu.Lock()
		node := r.Cluster.Nodes[r.Cluster.Ip]
		node.CpuUsage = usage
		r.Cluster.Nodes[r.Cluster.Ip] = node
		r.Cluster.Mu.Unlock()
		if usage >= 80.0 {
			// TODO: start new node
			cmd := exec.Command("./start_node.sh")
			out, err := cmd.Output()
			if err != nil {
				log.Print(err)
			}
			log.Println(string(out))
			count = 0
		} else if count >= 3 && usage < 80.0 {
			// TODO: shutdown another node
			cmd := exec.Command("./stop_node.sh")
			out, err := cmd.Output()
			if err != nil {
				log.Print(err)
			}
			log.Println(string(out))
			count = 0
		} else {
			count += 1
		}
		time.Sleep(10 * time.Second) // TODO: adapt sleep time
	}
}
