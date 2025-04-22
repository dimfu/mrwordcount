package main

import (
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
	"strings"
	"time"
)

const PORT = ":8080"
const REDUCER_AMT = 5

type TaskType int

type Args struct {
	ID      string
	Host    string
	Port    int
	Task    TaskType
	Payload []byte
}

type Reply struct {
	Message string
}

type TimeServer int64
type Master struct {
	clients map[string]TaskType
}

const (
	TASK_MAP TaskType = iota
	TASK_REDUCE
	TASK_UNDEFINED
)

func NewMaster() *Master {
	return &Master{
		clients: make(map[string]TaskType),
	}
}

func (t *TimeServer) GiveServerTime(args *Args, reply *int64) error {
	*reply = time.Now().Unix()
	return nil
}

func (m *Master) Register(args *Args, reply *string) error {
	h := fmt.Sprintf("%v:%d", args.Host, args.Port)
	log.Printf("new connection received: %s", h)
	m.clients[h] = args.Task
	*reply = "Registered to master"
	return nil
}

func (m *Master) Unregister(args *Args, reply *string) error {
	h := fmt.Sprintf("%v:%d", args.Host, args.Port)
	log.Printf("connection removed: %s", h)
	delete(m.clients, h)
	*reply = "Disconnected from master"
	return nil
}

func scanPg(pg string) (string, error) {
	file, err := os.Open("static")
	if err != nil {
		return "", fmt.Errorf("failed to open static dir: %w", err)
	}
	defer file.Close()
	files, _ := file.ReadDir(0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		ext := filepath.Ext(file.Name())
		name := strings.TrimSuffix(file.Name(), ext)
		if name == pg {
			return filepath.Join("static", file.Name()), nil
		}
	}
	return "", fmt.Errorf("file %s not found", pg)
}

func readPg(p string, wsize int) (<-chan []byte, error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	s, _ := file.Stat()
	chunkSize := int(s.Size()) / wsize
	out := make(chan []byte)
	leftOver := make([]byte, 0, chunkSize)

	go func() {
		defer file.Close()
		defer close(out)
		for {
			b := make([]byte, chunkSize)
			n, err := file.Read(b)
			if n == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				continue
			}

			// prepend leftover to current read
			b = append(leftOver, b[:n]...)
			n = len(b)

			// reset leftOver since it's already been prepended
			leftOver = leftOver[:0]

			// find last safe split that is not an alphabet/number/symbols
			split := n
			for i := n - 1; i >= 0; i-- {
				if b[i] == ' ' || b[i] == '\n' || b[i] == '\t' {
					split = i + 1
					break
				}
			}
			if split < n {
				leftOver = append(b[split:], leftOver...)
			}
			chunk := make([]byte, split)
			copy(chunk, b[:split])
			out <- chunk
		}
	}()
	if len(leftOver) > 0 {
		out <- leftOver
	}
	return out, nil
}

func main() {
	master := NewMaster()
	timeServer := new(TimeServer)

	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	rpc.RegisterName("TimeServer", timeServer)
	rpc.RegisterName("Master", master)
	rpc.HandleHTTP()

	// check worker health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		var msg []string
		for host := range master.clients {
			client, err := rpc.Dial("tcp", host)
			if err != nil {
				log.Println("error on dialing.", err)
				continue
			}
			defer client.Close()
			var reply string
			err = client.Call("Worker.Health", Args{}, &reply)
			if err != nil {
				log.Println("error on calling.", err)
				continue
			}
			msg = append(msg, fmt.Sprintf("%s: %s", host, reply))
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, strings.Join(msg, "\n"))
	})

	// handle word counts
	mux.HandleFunc("/count/{pg}", func(w http.ResponseWriter, r *http.Request) {
		pg := r.PathValue("pg")
		f, err := scanPg(pg)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		clientsLen := len(master.clients)
		if clientsLen == 0 {
			http.Error(w, "No clients to call", http.StatusServiceUnavailable)
			return
		}

		// distribute task
		reducersLen := int(math.Min(REDUCER_AMT, math.Max(1, float64(clientsLen-1))))
		mappersLen := clientsLen - reducersLen
		connectedClients := make(map[*rpc.Client]TaskType)
		var i int
		for clientAddr := range master.clients {
			client, err := rpc.Dial("tcp", clientAddr)
			if err != nil {
				log.Println("error on dialing.", err)
				continue
			}
			var t TaskType
			if i < mappersLen {
				connectedClients[client] = TASK_MAP
				t = TASK_MAP
			} else {
				connectedClients[client] = TASK_REDUCE
				t = TASK_REDUCE
			}
			var reply string
			err = client.Call("Worker.AssignTask", &Args{Task: t}, &reply)
			if err != nil {
				log.Println("error while assigning task", err)
				continue
			}
			fmt.Println(reply)
			i++
		}

		mapClients := []*rpc.Client{}
		for client, task := range connectedClients {
			if task == TASK_MAP {
				mapClients = append(mapClients, client)
			}
		}

		content, err := readPg(f, mappersLen)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		var j int
		for c := range content {
			client := mapClients[j]
			defer client.Close()
			var reply string
			err := client.Call("Worker.Map", &Args{Payload: c}, &reply)
			if err != nil {
				log.Println("error on map", err)
				continue
			}
			fmt.Println(reply)
			j++
		}
	})

	l, err := net.Listen("tcp", PORT)
	if err != nil {
		panic(err)
	}

	http.Serve(l, mux)
}
