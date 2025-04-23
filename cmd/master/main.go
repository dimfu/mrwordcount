package main

import (
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/dimfu/mrwordcount/shared"
)

const (
	PORT        = ":8080"
	REDUCER_AMT = 5
)

type Master struct {
	clients map[string]shared.TaskType
}

type TimeServer int64

func (t *TimeServer) GiveServerTime(args *shared.Args, reply *int64) error {
	*reply = time.Now().Unix()
	return nil
}

func main() {
	master := NewMaster()
	timeServer := new(shared.TimeServer)

	mux := master.Routes()

	rpc.RegisterName("TimeServer", timeServer)
	rpc.RegisterName("Master", master)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", PORT)
	if err != nil {
		panic(err)
	}

	http.Serve(l, mux)
}
