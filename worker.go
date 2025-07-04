package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const TEMP_DIR = "./_output/temp/"
const OUTPUT_DIR = "./_output/"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := uuid.NewString()

	log.Println("Worker - Intialized woker with ID ", workerID)

	args := TaskRequest{WorkerID: workerID}

	for {
		task, err := GetTask(&args)
		if err != nil {
			log.Println("Worker - Failed to reach Coordinator:", err)
			return
		}

		switch task.Type {
		case WAIT:
			log.Println("Worker - No task, waiting...")
			time.Sleep(4 * time.Second)

		case EXIT:
			log.Println("Worker - All work completed, exiting.")
			return

		case MAP:
			err := handleMapTask(task, mapf)
			if err != nil {
				log.Println("Worker - Error while executing MAP function", err)
				return
			}

			response := &Output{
				TaskID:   task.ID,
				WorkerID: workerID,
				TaskType: task.Type,
				BucketID: task.ID,
			}
			Notify(response)

		case REDUCE:
			err := handleReduceTask(task, reducef)
			if err != nil {
				log.Println("Worker - Error while executing REDUCE function", err)
				return
			}
			response := &Output{
				TaskID:   task.ID,
				WorkerID: workerID,
				TaskType: task.Type,
			}
			Notify(response)
		default:
			log.Println("Worker - Received unsupported Task Type", task.Type)
		}
	}
}

func GetTask(args *TaskRequest) (Task, error) {
	var task Task

	log.Println("Worker - Requesting Task...")

	ok := call("Coordinator.GetTask", args, &task)
	if !ok {
		return Task{}, fmt.Errorf("RPC call failed")
	}

	log.Println("Worker - Received Task:", task.ID, "Type:", task.Type)

	return task, nil
}

func Notify(payload *Output) error {

	log.Printf("Worker - Sending %s Response to Coordinator\n", payload.TaskType.ToString())

	ok := call("Coordinator.Notify", payload, &struct{}{})
	if !ok {
		return fmt.Errorf("RPC call failed")
	}

	log.Println("Worker - Response sent!")

	return nil
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) (err error) {

	log.Println("Worker - Initializing MAP Task")

	tmpDir := filepath.Join(TEMP_DIR, task.CoordinatorID)
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("Worker - REDUCE task %s panicked: %v\n", task.ID, rec)
			err = fmt.Errorf("map panic: %v", rec)
		}
	}()

	// Read the contents of the file
	content, err := openFile(task.FileName)
	if err != nil {
		return err
	}

	// Run the map function
	kva := mapf(task.FileName, content)

	numBuckets := task.NReduce
	type bucketWriter struct {
		tempFile  *os.File
		buffer    *bufio.Writer
		encoder   *json.Encoder
		tmpPath   string
		finalDir  string
		finalPath string
	}
	writers := make([]bucketWriter, numBuckets)

	for i := range numBuckets {
	
		finalDir := filepath.Join(tmpDir, strconv.Itoa(i))
		if err := os.MkdirAll(finalDir, 0755); err != nil {
			return fmt.Errorf("Worker - handleMapTask: mkdir %s: %w", finalDir, err)
		}

		// Temp file in the same direcotry
		tmpPath := filepath.Join(finalDir, fmt.Sprintf("mr-tmp-%s", task.ID))
		tmpFile, err := os.Create(tmpPath)
		if err != nil {
			return fmt.Errorf("Worker - handleMapTask: create temp file %s - %w", tmpPath, err)
		}

		buffer := bufio.NewWriter(tmpFile)
		encoder := json.NewEncoder(buffer)

		finalPath := filepath.Join(finalDir, fmt.Sprintf("mr-%s", task.ID))

		writers[i] = bucketWriter{
			tempFile:  tmpFile,
			buffer:    buffer,
			encoder:   encoder,
			tmpPath:   tmpPath,
			finalDir:  finalDir,
			finalPath: finalPath,
		}
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		if err := writers[bucket].encoder.Encode(&kv); err != nil {
			return fmt.Errorf("Worker - handleMapTask: encode kv %+v: %w", kv, err)
		}
	}

	// 5. Flush, close, and rename temp files to final names atomically
	for _, w := range writers {
		if err := w.buffer.Flush(); err != nil {
			return fmt.Errorf("Worker - handleMapTask: flush buffer: %w", err)
		}
		if err := w.tempFile.Close(); err != nil {
			return fmt.Errorf("Worker - handleMapTask: close temp file: %w", err)
		}
		if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
			return fmt.Errorf("Worker - handleMapTask: rename to final %s: %w", w.finalPath, err)
		}
	}

	log.Printf("Worker - MAP Task %s Completed!", task.ID)
	return nil
}

func handleReduceTask(task Task, reducef func(string, []string) string) (err error) {
	log.Println("Worker - Initializing REDUCE task")

	// 1. Get all intermediate files for this bucket
	fmt.Println(task.BucketID)
	bucketDir := filepath.Join(TEMP_DIR, task.CoordinatorID, fmt.Sprintf("%d", task.BucketID))
	pattern := filepath.Join(bucketDir, "mr-*")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("handleReduceTask: glob %s: %w", pattern, err)
	}
	if len(files) == 0 {
		log.Printf("handleReduceTask: no intermediate files found for %s\n", task.ID)
	}

	// 2. Load and decode all key-value pairs
	kvs := make(map[string][]string)
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return fmt.Errorf("handleReduceTask: open %s: %w", fname, err)
		}
		defer f.Close()

		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("handleReduceTask: decode %s: %w", fname, err)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	// 3. Prepare output file atomically
	if err := os.MkdirAll(OUTPUT_DIR, 0755); err != nil {
		return fmt.Errorf("handleReduceTask: mkdir %s: %w", OUTPUT_DIR, err)
	}

	// Final and temp output paths
	finalOutPath := filepath.Join(OUTPUT_DIR, fmt.Sprintf("mr-out-%s", task.ID))
	tmpOutFile, err := os.Create(finalOutPath)
	if err != nil {
		return fmt.Errorf("handleReduceTask: create temp file: %w", err)
	}
	defer func() {
		tmpOutFile.Close()
		if err != nil {
			os.Remove(tmpOutFile.Name())
		}
	}()

	// 4. Reduce all keys
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	writer := bufio.NewWriter(tmpOutFile)
	for _, k := range keys {
		result := reducef(k, kvs[k])
		if _, err := fmt.Fprintf(writer, "%v %v\n", k, result); err != nil {
			return fmt.Errorf("handleReduceTask: write %s: %w", k, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("handleReduceTask: flush: %w", err)
	}

	// 5. Atomic rename to final output file
	if err := os.Rename(tmpOutFile.Name(), filepath.Join(OUTPUT_DIR, fmt.Sprintf("mr-out-%d", task.BucketID))); err != nil {
		return fmt.Errorf("handleReduceTask: rename to final output: %w", err)
	}

	log.Printf("Worker - Completed REDUCE task %s → %s\n", task.ID, finalOutPath)
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// was probably: log.Println("dialing:", err); return false
		log.Fatalf("Worker - dialing Coordinator socket failed: %v", err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		log.Printf("Worker - RPC %s error: %v", rpcname, err)
		return false
	}
	return true
}
