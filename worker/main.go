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
)

type Args struct {
	Host    string
	Port    int
	Task    string
	Payload []byte
}
type Reply struct {
	Message string
}
type Worker struct {
	Task string
}

const MASTER_PORT = 8080

const (
	TASK_MAP    = "map"
	TASK_REDUCE = "reduce"
)

func initWorker(task string) *Worker {
	return &Worker{
		Task: task,
	}
}

func (w *Worker) Health(args *Args, reply *string) error {
	*reply = "OK"
	return nil
}

func (w *Worker) Map(args *Args, reply *map[string]int) error {
	m := make(map[string]int)
	str := string(args.Payload)
	words := strings.Fields(str)
	for _, word := range words {
		m[word]++
	}
	*reply = m
	return nil
}

var (
	port int
	task string
)

func main() {
	flag.IntVar(&port, "p", 9000, "Provide port number")
	flag.StringVar(&task, "t", TASK_MAP, "Worker task (map/reduce)")
	flag.Parse()

	if task != "map" && task != "reduce" {
		fmt.Println("task should be 'map' or 'reduce'")
		return
	}

	args := Args{}

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

	err := client.Call("Master.Register", &Args{Host: "localhost", Port: port, Task: task}, new(string))
	if err != nil {
		log.Fatalf("failed registering connection to master: %v", err)
	}

	worker := initWorker(task)
	rpc.Register(worker)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	go rpc.Accept(l)

	select {
	case <-stop:
		var reply string
		log.Println("received shutdown")
		err := client.Call("Master.Unregister", Args{Host: "localhost", Port: port}, &reply)
		if err != nil {
			log.Fatalf("failed registering connection to master: %v", err)
		} else {
			log.Println(reply)
		}
	}
}
