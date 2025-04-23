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
	"time"

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

	args := shared.Args{}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	var client *rpc.Client
	connected := false
	for !connected {
		select {
		case <-stop:
			log.Println("received shutdown on connection retry")
			return
		case <-time.After(2 * time.Second):
			addr := fmt.Sprintf("localhost:%d", MASTER_PORT)
			c, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				log.Println("err dialing:", err)
				time.Sleep(2 * time.Second)
				continue
			}
			client = c
			var reply int64
			err = client.Call("TimeServer.GiveServerTime", args, &reply)
			if err != nil {
				log.Println("arith error:", err)
				time.Sleep(2 * time.Second)
				continue
			}
			fmt.Println("connected to server:", reply)
			connected = true
		}
	}

	worker := initWorker(shared.TASK_UNDEFINED)
	rpc.Register(worker)

	err := client.Call("Master.Register", &shared.Args{Host: "localhost", Port: port, Task: worker.Task}, new(string))
	if err != nil {
		log.Fatalf("failed registering connection to master: %v", err)
	}
	worker.Addr = fmt.Sprintf("%s:%d", "localhost", port)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	go rpc.Accept(l)

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
