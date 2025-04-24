package main

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"

	"github.com/dimfu/mrwordcount/shared"
)

type Master struct {
	clients           map[string]shared.TaskType
	mapAssignments    map[*rpc.Client]*taskInfo
	reduceAssignments map[*rpc.Client]*taskInfo
	wg                sync.WaitGroup
}

func newMaster() *Master {
	return &Master{
		clients:           make(map[string]shared.TaskType),
		mapAssignments:    make(map[*rpc.Client]*taskInfo),
		reduceAssignments: make(map[*rpc.Client]*taskInfo),
	}
}

func (m *Master) runServer() error {
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		return fmt.Errorf("error while listening to port %s: %v", PORT, err)
	}

	return http.Serve(l, m.routes())
}

// rpc
func (m *Master) Register(args *shared.Args, reply *string) error {
	h := fmt.Sprintf("%v:%d", args.Host, args.Port)
	log.Printf("new connection received: %s", h)
	m.clients[h] = args.Task
	*reply = "Registered to master"
	return nil
}

func (m *Master) Unregister(args *shared.Args, reply *string) error {
	h := fmt.Sprintf("%v:%d", args.Host, args.Port)
	log.Printf("connection removed: %s", h)
	delete(m.clients, h)
	*reply = "Disconnected from master"
	return nil
}

// end rpc

func (m *Master) runMapper(content <-chan []byte, filename string) []string {
	fileNames := []string{}
	clients := []*rpc.Client{}
	for c := range m.mapAssignments {
		clients = append(clients, c)
	}
	var j int32
	var success, fail int32
	var wg sync.WaitGroup
	var mu sync.Mutex
	for c := range content {
		wg.Add(1)
		go func(payload []byte) {
			defer wg.Done()
			mu.Lock()
			idx := atomic.AddInt32(&j, 1) - 1
			client := clients[idx]
			var taskPayload shared.TaskDone
			err := client.Call("Worker.Map", &shared.Args{Payload: payload, Filename: filename}, &taskPayload)
			if err != nil || len(taskPayload.FileNames) == 0 {
				atomic.AddInt32(&fail, 1)
				log.Println("Map failed:", err)
				return
			}
			atomic.AddInt32(&success, 1)
			fileNames = append(fileNames, taskPayload.FileNames...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	log.Printf("[Mapper] Success: %d, Failed: %d", success, fail)
	return fileNames
}

func (m *Master) runReducer(fileName string) []string {
	fileNames := []string{}
	clients := []*rpc.Client{}
	for c := range m.reduceAssignments {
		clients = append(clients, c)
	}
	var success, fail int32
	var wg sync.WaitGroup
	var mu sync.Mutex

	for client, task := range m.reduceAssignments {
		wg.Add(1)
		go func(t *taskInfo) {
			defer wg.Done()
			mu.Lock()
			var p string
			err := client.Call("Worker.Reduce", &shared.Args{FileNames: t.fileNames, Filename: fileName}, &p)
			if err != nil || len(p) == 0 {
				atomic.AddInt32(&fail, 1)
				log.Println("Reduce failed:", err)
				return
			}
			atomic.AddInt32(&success, 1)
			fileNames = append(fileNames, p)
			mu.Unlock()
		}(task)
	}
	wg.Wait()
	log.Printf("[Reducer] Success: %d, Failed: %d", success, fail)
	return fileNames
}

func (m *Master) distributeTask() error {
	nClient := len(m.clients)
	if nClient == 0 {
		return errors.New("No clients to call")
	}
	nReducer := 1
	if nClient > REDUCER_AMT {
		nReducer = int(math.Min(REDUCER_AMT, math.Max(1, float64(nClient-1))))
	}
	nMapper := nClient - nReducer
	var i int
	for clientAddr := range m.clients {
		client, err := rpc.Dial("tcp", clientAddr)
		if err != nil {
			log.Println("error on dialing.", err)
			continue
		}
		var t shared.TaskType
		if i < nMapper {
			m.mapAssignments[client] = &taskInfo{
				status: shared.IN_PROGRESS,
			}
			t = shared.TASK_MAP
		} else {
			m.reduceAssignments[client] = &taskInfo{
				status: shared.IN_PROGRESS,
			}
			t = shared.TASK_REDUCE
		}
		var reply string
		err = client.Call("Worker.AssignTask", &shared.Args{Task: t, NReducer: nReducer}, &reply)
		if err != nil {
			client.Close()
			log.Println("error while assigning task", err)
			continue
		}
		log.Println(reply)
		i++
	}
	return nil
}

func (m *Master) clearAssignments() {
	for client := range m.mapAssignments {
		client.Close()
		delete(m.mapAssignments, client)
	}
	for client := range m.reduceAssignments {
		client.Close()
		delete(m.reduceAssignments, client)
	}
}
