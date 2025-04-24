package main

import (
	"net/http"
	"net/rpc"
)

func (m *Master) routes() http.Handler {
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	rpc.RegisterName("TimeServer", new(TimeServer))
	rpc.RegisterName("Master", m)
	rpc.HandleHTTP()

	healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			m.health(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	countHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			m.count(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.Handle("/health", healthHandler)
	mux.Handle("/count/{pg}", countHandler)
	return mux
}
