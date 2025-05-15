package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
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
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/dimfu/mrwordcount/shared"
)

type Worker struct {
	Addr         string
	NReducer     int
	State        shared.TaskStatus
	Task         shared.TaskType
	IntentFailed bool
}

const MASTER_PORT = 8080

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func initWorker(task shared.TaskType, intentFailed bool) *Worker {
	return &Worker{
		Task:         task,
		State:        shared.IDLE,
		IntentFailed: intentFailed,
	}
}

func (w *Worker) resetState() {
	w.Task = shared.TASK_UNDEFINED
	w.State = shared.IDLE
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
	rep := fmt.Sprintf("[%v] Assigned task %v", w.Addr, t)
	log.Println(rep)
	*reply = rep
	return nil
}

func (w *Worker) ClearTask(args *shared.Args, reply *string) error {
	w.resetState()
	return nil
}

// rpc
func (w *Worker) AckWorker(args *shared.Args, reply *shared.AckWorker) error {
	*reply = shared.AckWorker{Ack: true, TaskType: w.Task}
	return nil
}

func (w *Worker) Health(args *shared.Args, reply *string) error {
	*reply = "OK"
	return nil
}

func (w *Worker) Map(args *shared.Args, reply *shared.TaskDone) error {
	defer w.resetState()
	if w.IntentFailed {
		return errors.New("Job failed intentionally")
	}
	start := time.Now()
	splitFunc := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		start := 0
		for start < len(data) {
			if unicode.IsLetter(rune(data[start])) {
				break
			}
			start++
		}
		for i := start; i < len(data); i++ {
			if !unicode.IsLetter(rune(data[i])) {
				return i + 1, data[start:i], nil
			}
		}
		if atEOF && start < len(data) {
			return len(data), data[start:], nil
		}
		return start, nil, nil
	}

	baseTmp := os.TempDir()
	mapTmpDir := filepath.Join(baseTmp, args.Filename)
	err := os.MkdirAll(mapTmpDir, 0755)
	if err != nil {
		return err
	}

	intermediateFileNames := []string{}
	encoders := make(map[int]*json.Encoder)
	files := make([]*os.File, w.NReducer)

	for i := 0; i < w.NReducer; i++ {
		f, err := os.CreateTemp(mapTmpDir, "map")
		if err != nil {
			return err
		}
		files[i] = f
		intermediateFileNames = append(intermediateFileNames, f.Name())
		enc := json.NewEncoder(f)
		encoders[i] = enc
	}

	reader := bytes.NewReader(args.Payload)
	scanner := bufio.NewScanner(reader)
	scanner.Split(splitFunc)

	for scanner.Scan() {
		word := scanner.Text()
		kv := shared.KV{Key: word, Value: 1}
		err := encoders[ihash(kv.Key)%w.NReducer].Encode(&kv)
		if err != nil {
			return fmt.Errorf("couldn't encode %v: %v", &kv, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %v", err)
	}

	for _, f := range files {
		f.Close()
	}

	*reply = shared.TaskDone{
		FileNames: intermediateFileNames,
	}

	elapsed := time.Since(start)
	log.Println("Map took", elapsed)

	return nil
}

func (w *Worker) Reduce(args *shared.Args, reply *string) error {
	defer w.resetState()
	if w.IntentFailed {
		return errors.New("Job failed intentionally")
	}
	start := time.Now()
	log.Println("Reduce in process")
	counts := make(map[string]int)
	for _, name := range args.FileNames {
		err := func() error {
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
				counts[strings.ToLower(kv.Key)] += kv.Value
			}
			return nil
		}()
		if err != nil {
			return err
		}
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
	elapsed := time.Since(start)
	log.Println("Reduce took", elapsed)
	return nil
}

// end rpc

func main() {
	var port int
	var intentFailed bool
	flag.IntVar(&port, "p", 9000, "Provide port number")
	flag.BoolVar(&intentFailed, "if", false, "Intentionally fail the worker's job")
	flag.Parse()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	worker := initWorker(shared.TASK_UNDEFINED, intentFailed)
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
