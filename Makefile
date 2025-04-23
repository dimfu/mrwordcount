build:
	go build -o ./bin/master ./cmd/master && go build -o ./bin/worker ./cmd/worker

run-master:
	./bin/master

run-worker:
	./bin/worker
