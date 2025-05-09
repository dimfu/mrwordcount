package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func baseName(p string) string {
	base := filepath.Base(p)
	ext := filepath.Ext(base)
	if len(ext) > 0 {
		return strings.TrimSuffix(base, ext)
	}
	return base
}

func getFilePath(t string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	file, err := os.Open(path.Join(dir, "static"))
	if err != nil {
		return "", fmt.Errorf("failed to open static dir: %w", err)
	}
	defer file.Close()
	files, _ := file.ReadDir(0)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		ext := filepath.Ext(file.Name())
		name := strings.TrimSuffix(file.Name(), ext)
		if name == t {
			// TODO: do something that isn't hardcoded to only static folder
			return filepath.Join("static", file.Name()), nil
		}
	}
	return "", fmt.Errorf("file %s not found", t)
}

func readChunks(p string, wsize int) (<-chan []byte, error) {
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	s, _ := file.Stat()
	chunkSize := int(s.Size()) / wsize
	out := make(chan []byte)
	leftOver := make([]byte, 0, chunkSize)

	go func() {
		defer file.Close()
		defer close(out)
		for {
			b := make([]byte, chunkSize)
			n, err := file.Read(b)
			if n == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				continue
			}

			// prepend leftover to current read
			b = append(leftOver, b[:n]...)
			n = len(b)

			// reset leftOver since it's already been prepended
			leftOver = leftOver[:0]

			// find last safe split that is not an alphabet/number/symbols
			split := n
			for i := n - 1; i >= 0; i-- {
				if b[i] == ' ' || b[i] == '\n' || b[i] == '\t' {
					split = i + 1
					break
				}
			}
			if split < n {
				leftOver = append(b[split:], leftOver...)
			}
			chunk := make([]byte, split)
			copy(chunk, b[:split])
			out <- chunk
		}
		if len(leftOver) > 0 {
			out <- leftOver
		}
	}()
	return out, nil
}
