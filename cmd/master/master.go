package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.ackWorkers()
			}
		}
	}()

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
	var success, fail int32
	var wg sync.WaitGroup
	var mu sync.Mutex

	var retryQueue [][]byte

	for c := range content {
		wg.Add(1)
		go func(payload []byte) {
			defer wg.Done()
			var client *rpc.Client
			mu.Lock()
			for mapper, t := range m.mapAssignments {
				if t.status == shared.IDLE {
					t.status = shared.IN_PROGRESS
					client = mapper
					break
				}
			}
			mu.Unlock()
			if client == nil {
				return
			}
			var taskPayload shared.TaskDone
			err := client.Call("Worker.Map", &shared.Args{Payload: payload, Filename: filename}, &taskPayload)
			if err != nil || len(taskPayload.FileNames) == 0 {
				atomic.AddInt32(&fail, 1)
				log.Println("Map failed:", err)
				mu.Lock()
				retryQueue = append(retryQueue, payload)
				delete(m.mapAssignments, client)
				mu.Unlock()
				return
			}
			atomic.AddInt32(&success, 1)
			mu.Lock()
			m.mapAssignments[client].status = shared.IDLE
			fileNames = append(fileNames, taskPayload.FileNames...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	log.Printf("[Mapper] Success: %d, Failed: %d", success, fail)

	if len(retryQueue) > 0 {
		log.Printf("[Mapper] Retrying %d failed tasks...", len(retryQueue))
		retryChan := make(chan []byte, len(retryQueue))
		for _, item := range retryQueue {
			retryChan <- item
		}
		close(retryChan)
		retriedResults := m.runMapper(retryChan, filename)
		fileNames = append(fileNames, retriedResults...)
	}

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
			for _, file := range t.fileNames {
				if err := os.Remove(file); err != nil {
					log.Println(err)
				}
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
				status:     shared.IDLE,
				identifier: clientAddr,
			}
			t = shared.TASK_MAP
		} else {
			m.reduceAssignments[client] = &taskInfo{
				status:     shared.IDLE,
				identifier: clientAddr,
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

func (m *Master) ackWorkers() {
	var clients []*rpc.Client
	for client := range m.mapAssignments {
		clients = append(clients, client)
	}
	for client := range m.reduceAssignments {
		clients = append(clients, client)
	}
	for _, client := range clients {
		var ack = shared.AckWorker{}
		err := client.Call("Worker.AckWorker", &shared.Args{}, &ack)
		if err != nil || !ack.Ack {
			log.Println("failed on acknowledging client:", err)
			continue
		}
	}
}

func (m *Master) clearAllAssignments() {
	for client := range m.mapAssignments {
		client.Close()
		delete(m.mapAssignments, client)
	}
	for client := range m.reduceAssignments {
		client.Close()
		delete(m.reduceAssignments, client)
	}
}

func (m *Master) archive(files []string) ([]byte, error) {
	var buf bytes.Buffer
	if len(files) == 0 {
		return buf.Bytes(), errors.New("No file to archive")
	}
	writer := zip.NewWriter(&buf)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return nil, err
		}
		header.Name = filepath.Base(file)
		w, err := writer.CreateHeader(header)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(w, f)
		if err != nil {
			return nil, err
		}
		if err := os.Remove(f.Name()); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
