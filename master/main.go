package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const PORT = ":8080"

type Args struct {
	ID      string
	Host    string
	Port    int
	Task    string
	Payload []byte
}

type Reply struct {
	Message string
}

type TimeServer int64
type Master struct {
	clients map[string]string
}

func NewMaster() *Master {
	return &Master{
		clients: make(map[string]string),
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

		mappers := []string{}
		for k, v := range master.clients {
			if v == "map" {
				mappers = append(mappers, k)
			}
		}

		if len(mappers) == 0 {
			http.Error(w, "No clients to call", http.StatusServiceUnavailable)
			return
		}

		// TODO: diffrentiate which client is which (mapper or reducer)
		content, err := readPg(f, len(mappers))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		var i int
		for c := range content {
			host := mappers[i]
			client, err := rpc.Dial("tcp", host)
			if err != nil {
				log.Println("error on dialing.", err)
				continue
			}
			defer client.Close()
			var reply map[string]int
			err = client.Call("Worker.Map", Args{Payload: c}, &reply)
			if err != nil {
				log.Println("error on calling.", err)
				continue
			}
			fmt.Println(reply)
			i++
		}
	})

	l, err := net.Listen("tcp", PORT)
	if err != nil {
		panic(err)
	}

	http.Serve(l, mux)
}
