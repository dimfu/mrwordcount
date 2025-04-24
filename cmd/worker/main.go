package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"maps"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"unicode"

	"github.com/dimfu/mrwordcount/shared"
)

type Worker struct {
	Addr     string
	NReducer int
	State    shared.TaskStatus
	Task     shared.TaskType
}

const MASTER_PORT = 8080

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func initWorker(task shared.TaskType) *Worker {
	return &Worker{
		Task:  task,
		State: shared.IDLE,
	}
}

func (w *Worker) Health(args *shared.Args, reply *string) error {
	*reply = "OK"
	return nil
}

func (w *Worker) AssignTask(args *shared.Args, reply *string) error {
	w.Task = args.Task
	w.NReducer = args.NReducer
	var t string
	switch args.Task {
	case shared.TASK_MAP:
		t = "Map"
	case shared.TASK_REDUCE:
		t = "Reduce"
	}
	*reply = fmt.Sprintf("[%v] Assigned task %v", w.Addr, t)
	return nil
}

func (w *Worker) Map(args *shared.Args, reply *shared.TaskDone) error {
	payload := string(args.Payload)
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(payload, ff)
	intermediateKV := []shared.KV{}
	for _, word := range words {
		kv := shared.KV{Key: word, Value: 1}
		intermediateKV = append(intermediateKV, kv)
	}
	encoders := make(map[int]*json.Encoder)
	intermediateFileNames := []string{}

	baseTmp := os.TempDir()
	mapTmpDir := filepath.Join(baseTmp, args.Filename)
	err := os.MkdirAll(mapTmpDir, 0755)
	if err != nil {
		return err
	}

	for i := 0; i < w.NReducer; i++ {
		f, err := os.CreateTemp(mapTmpDir, "map")
		if err != nil {
			return err
		}
		defer f.Close()
		intermediateFileNames = append(intermediateFileNames, f.Name())
		enc := json.NewEncoder(f)
		encoders[i] = enc
	}

	for _, kv := range intermediateKV {
		err := encoders[ihash(kv.Key)%w.NReducer].Encode(&kv)
		if err != nil {
			return fmt.Errorf("couldn't encode %v\n", &kv)
		}
	}
	*reply = shared.TaskDone{
		FileNames: intermediateFileNames,
	}
	return nil
}

func (w *Worker) Reduce(args *shared.Args, reply *string) error {
	intermediate := []shared.KV{}
	for _, name := range args.FileNames {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		decoder := json.NewDecoder(f)
		for {
			var kv shared.KV
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(shared.ByKey(intermediate))

	counts := make(map[string]int)
	for _, kv := range intermediate {
		counts[strings.ToLower(kv.Key)] += kv.Value
	}

	baseTmp := os.TempDir()
	mapTmpDir := filepath.Join(baseTmp, args.Filename)
	err := os.MkdirAll(mapTmpDir, 0755)
	if err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(mapTmpDir, fmt.Sprintf("reduced-%v", args.Filename))
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	for _, key := range slices.Sorted(maps.Keys(counts)) {
		fmt.Fprintf(tmpFile, "%v %v\n", key, counts[key])
	}

	*reply = tmpFile.Name()
	return nil
}

func main() {
	var port int
	flag.IntVar(&port, "p", 9000, "Provide port number")
	flag.Parse()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	worker := initWorker(shared.TASK_UNDEFINED)
	rpc.Register(worker)

	var l net.Listener
	for {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if syscallErr, ok := opErr.Err.(*os.SyscallError); ok {
					if syscallErr.Err == syscall.EADDRINUSE {
						fmt.Printf("Port %d in use, trying next...\n", port)
						port++
						continue
					}
				}
			}
			panic(err)
		}
		l = listener
		break
	}

	addr := fmt.Sprintf("localhost:%d", MASTER_PORT)
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatalf("err dialing: %v", err)
	}
	var reply string
	err = client.Call("Master.Register", &shared.Args{Host: "localhost", Port: port, Task: worker.Task}, &reply)
	if err != nil {
		log.Fatalf("failed registering connection to master: %v", err)
	}
	log.Println(reply)

	worker.Addr = fmt.Sprintf("%s:%d", "localhost", port)

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
					return
				}
				log.Printf("Accept error: %v\n", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	select {
	case <-stop:
		var reply string
		log.Println("received shutdown")
		err := client.Call("Master.Unregister", shared.Args{Host: "localhost", Port: port}, &reply)
		if err != nil {
			log.Fatalf("failed registering connection to master: %v", err)
		} else {
			log.Println(reply)
		}
	}
}
