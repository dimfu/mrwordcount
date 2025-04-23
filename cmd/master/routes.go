package main

import "net/http"

func (m *Master) Routes() http.Handler {
	mux := http.NewServeMux()
	http.DefaultServeMux = mux

	healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			m.Health(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	countHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			m.Count(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.Handle("/health", healthHandler)
	mux.Handle("/count/{pg}", countHandler)
	return mux
}
