package main

import (
	"time"

	"github.com/dimfu/mrwordcount/shared"
)

const (
	PORT        = ":8080"
	REDUCER_AMT = 5
)

type taskInfo struct {
	identifier string
	status     shared.TaskStatus
	fileNames  []string
}

type TimeServer int64

func (t *TimeServer) GiveServerTime(args *shared.Args, reply *int64) error {
	*reply = time.Now().Unix()
	return nil
}

func main() {
	m := newMaster()
	if err := m.runServer(); err != nil {
		panic(err)
	}
}
