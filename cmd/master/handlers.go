package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dimfu/mrwordcount/shared"
)

func (m *Master) Health(w http.ResponseWriter, r *http.Request) {
	var msg []string
	for host := range m.clients {
		client, err := rpc.Dial("tcp", host)
		if err != nil {
			log.Println("error on dialing.", err)
			continue
		}
		defer client.Close()
		var reply string
		err = client.Call("Worker.Health", shared.Args{}, &reply)
		if err != nil {
			log.Println("error on calling.", err)
			continue
		}
		msg = append(msg, fmt.Sprintf("%s: %s", host, reply))
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, strings.Join(msg, "\n"))
}

func (m *Master) Count(w http.ResponseWriter, r *http.Request) {
	pg := r.PathValue("pg")
	f, err := GetFilePath(pg)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	clientsLen := len(m.clients)
	if clientsLen == 0 {
		http.Error(w, "No clients to call", http.StatusServiceUnavailable)
		return
	}

	// distribute task
	reducersLen := 1
	if clientsLen > REDUCER_AMT {
		reducersLen = int(math.Min(REDUCER_AMT, math.Max(1, float64(clientsLen-1))))
	}
	mappersLen := clientsLen - reducersLen
	connectedClients := make(map[*rpc.Client]shared.TaskType)
	var i int
	for clientAddr := range m.clients {
		client, err := rpc.Dial("tcp", clientAddr)
		if err != nil {
			log.Println("error on dialing.", err)
			continue
		}
		var t shared.TaskType
		if i < mappersLen {
			connectedClients[client] = shared.TASK_MAP
			t = shared.TASK_MAP
		} else {
			connectedClients[client] = shared.TASK_REDUCE
			t = shared.TASK_REDUCE
		}
		var reply string
		err = client.Call("Worker.AssignTask", &shared.Args{Task: t}, &reply)
		if err != nil {
			log.Println("error while assigning task", err)
			continue
		}
		fmt.Println(reply)
		i++
	}

	mapClients := []*rpc.Client{}
	reducerClients := []*rpc.Client{}
	for client, task := range connectedClients {
		if task == shared.TASK_MAP {
			mapClients = append(mapClients, client)
		} else {
			reducerClients = append(reducerClients, client)
		}
	}

	content, err := ReadChunks(f, mappersLen)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	var j int32
	var wg sync.WaitGroup
	for c := range content {
		wg.Add(1)
		go func(payload []byte) {
			defer wg.Done()
			idx := atomic.AddInt32(&j, 1) - 1
			client := mapClients[idx]
			defer client.Close()
			var reply string
			err := client.Call("Worker.Map", &shared.Args{Payload: payload}, &reply)
			if err != nil {
				log.Println("error on map", err)
				return
			}
			fmt.Println(reply)
		}(c)
	}
	wg.Wait()
	fmt.Println("Done processing text from all mapper")
}
