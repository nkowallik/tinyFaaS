package http

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/OpenFogStack/tinyFaaS/pkg/rproxy"
)

func Start(r *rproxy.RProxy, listenAddr string) {

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		p := req.URL.Path

		for p != "" && p[0] == '/' {
			p = p[1:]
		}

		async := req.Header.Get("X-tinyFaaS-Async") != ""

		// log.Printf("have request for path: %s (async: %v)", p, async)

		req_body, err := io.ReadAll(req.Body)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Print(err)
			return
		}

		if req.Header.Get("X-tinyFaaS-register") != "" {
			log.Printf("Registering: %s", req.Header.Get("X-tinyFaaS-register"))
			node, err := r.AddTFInstance(req.Header.Get("X-tinyFaaS-register"), req_body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				log.Print(err)
				return
			}
			json.NewEncoder(w).Encode(node)
			return
		}

		var joincluster = req.Header.Get("X-tinyFaaS-joincluster")
		if joincluster != "" {
			log.Printf("Joining cluster: %s", joincluster)
			err := r.RegisterInCluster(joincluster)
			if err != nil {
				log.Fatal("Bad Request Response", err)
				w.WriteHeader(http.StatusBadRequest)
				log.Print(err)
				return
			}
			log.Println("Respond OK!")
			log.Println(r.Cluster)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		nodeAddr := req.Header.Get("X-tinyFaaS-status")
		if nodeAddr != "" {
			log.Printf("Updating Node Status")
			err := r.UpdateClusterNode(nodeAddr, req_body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				log.Print(err)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}

		headers := make(map[string]string)
		for k, v := range req.Header {
			headers[k] = v[0]
		}

		s, res := r.Call(p, req_body, async, headers)

		switch s {
		case rproxy.StatusOK:
			w.WriteHeader(http.StatusOK)
			w.Write(res)
		case rproxy.StatusAccepted:
			w.WriteHeader(http.StatusAccepted)
		case rproxy.StatusNotFound:
			w.WriteHeader(http.StatusNotFound)
		case rproxy.StatusError:
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	log.Printf("Starting HTTP server on %s", listenAddr)
	err := http.ListenAndServe(listenAddr, mux)

	if err != nil {
		log.Fatal(err)
	}

	log.Print("HTTP server stopped")

}
