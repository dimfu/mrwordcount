package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strings"
	"time"

	"github.com/dimfu/mrwordcount/shared"
)

func (m *Master) health(w http.ResponseWriter, _ *http.Request) {
	var msg []string
	for host := range m.clients {
		client, err := rpc.Dial("tcp", host)
		if err != nil {
			log.Println("error on dialing.", err)
			continue
		}
		defer client.Close()
		var reply string
		err = client.Call("Worker.Health", shared.Args{}, &reply)
		if err != nil {
			log.Println("error on calling.", err)
			continue
		}
		msg = append(msg, fmt.Sprintf("%s: %s", host, reply))
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, strings.Join(msg, "\n"))
}

func (m *Master) count(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	pg := r.PathValue("pg")
	f, err := getFilePath(pg)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println(err)
		}
		return
	}

	err = m.distributeTask()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	content, err := readChunks(f, len(m.mapAssignments))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	m.wg.Add(1)
	defer m.wg.Done()
	filename := baseName(f)
	intermediateFileNames := m.runMapper(content, filename)
	if len(intermediateFileNames) == 0 {
		err = fmt.Errorf("Cannot proceed to reduce phase, none of the mappers succeded")
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("Done processing text from all mapper, entering reducer phase now")

	taskPer := len(intermediateFileNames) / len(m.reduceAssignments)
	extra := len(intermediateFileNames) % len(m.reduceAssignments)

	pop := func(n int, arr *[]string) []string {
		i := 0
		if i+n > len(*arr) {
			n = len(*arr) - i
		}
		popped := (*arr)[i : i+n]
		*arr = append((*arr)[:i], (*arr)[i+n:]...)
		return popped
	}

	rcount := 0
	for _, info := range m.reduceAssignments {
		t := taskPer
		if rcount < extra {
			t += 1
		}
		assigned := pop(taskPer+1, &intermediateFileNames)
		info.fileNames = append(info.fileNames, assigned...)
		rcount++
	}

	results := m.runReducer(filename)
	buf, err := m.archive(results)
	if err != nil {
		http.Error(w, "Failed to create zip", http.StatusInternalServerError)
		return
	}

	if err := os.Remove(path.Join(os.TempDir(), filename)); err != nil {
		log.Println("Error while deleting temp dir")
	}

	elapsed := time.Since(start)
	log.Printf("Processes took %s to finish", elapsed)
	m.clearAssignments()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="archive.zip"`)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(buf)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf)
}
