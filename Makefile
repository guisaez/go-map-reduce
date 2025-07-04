.PHONY: coordinator, worker

coordinator:
	go run ./cmd/mrcoordinator/main.go ./input/pg*.txt

worker:
	go build -buildmode=plugin -o ./_build/wc.so mrapps/wc/wc.go
	go run cmd/mrworker/mrworker.go _build/wc.so

spawn_worker:
	go run cmd/mrworker/mrworker.go _build/wc.so

test:
	./test-mr.sh