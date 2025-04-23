package main

import (
	"fmt"
	"log"

	"github.com/dimfu/mrwordcount/shared"
)

func NewMaster() *Master {
	return &Master{
		clients: make(map[string]shared.TaskType),
	}
}

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
