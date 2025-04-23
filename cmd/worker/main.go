package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dimfu/mrwordcount/shared"
)

type Worker struct {
	Addr  string
	State string
	Task  shared.TaskType
}

const MASTER_PORT = 8080

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

func (w *Worker) Map(args *shared.Args, reply *string) error {
	m := make(map[string]int)
	str := string(args.Payload)
	words := strings.Fields(str)
	w.State = shared.PROCESSING
	*reply = fmt.Sprintf("[%v] Processing text...", w.Addr)
	for _, word := range words {
		m[word]++
	}
	w.State = shared.FINISH
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
