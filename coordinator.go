package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Coordinator struct {
	ID        string // CoordinatorID
	NReduce   int    // Number of reduce tasks
	Processes map[string]WorkState

	// Counters in this case are redundant but they are used for performace reasons :)
	// In a real world scenario having a larger number of tasks would result in higher computational overhead
	TotalMapTasks    int  // Total Number of map tasks
	NMapsCompleted   int  // Number of completed map Tasks
	NReduceCompleted int  // Number of completed reduce Tasks
	CanReduce        bool // Indicates of the reduce phase can be started

	mux sync.Mutex
}

type WorkState struct {
	TaskID    string    // Task ID
	WorkerID  string    // Worker ID
	Type      TaskType  // Work type MAP | REDUCE
	StartedAt time.Time // Time the task started
	Done      bool      // Indicates if the task is Done
	
	// Args
	FileName string
	BucketID int
}

// Worker requests a Task to the Coordinator
func (c *Coordinator) GetTask(args *TaskRequest, reply *Task) error {
	log.Println("Coordinator - Recevied Task Request from Worker:", args.WorkerID)

	if c.Done() {
		reply.Type = EXIT
		return nil
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	now := time.Now()

	switch c.CanReduce {
	case false:
		if assignedTask := c.scanAndAssign(args.WorkerID, now, MAP, func(id string, st WorkState) {
			// This closure "captures" the `reply` variable from the outer scope
			reply.ID = id
			reply.FileName = st.FileName
			reply.NReduce = c.NReduce
			reply.Type = MAP
			reply.CoordinatorID = c.ID
		}); assignedTask {
			log.Println("Coordinator - Assigned MAP task to", args.WorkerID, "-", reply.ID)
			return nil
		}
	case true:
		if assignedTask := c.scanAndAssign(args.WorkerID, now, REDUCE, func(id string, st WorkState) {
			reply.ID = id
			reply.BucketID = st.BucketID
			reply.Type = REDUCE
			reply.CoordinatorID = c.ID
		}); assignedTask {
			log.Println("Coordinator - Assigned REDUCE task to", args.WorkerID, "-", reply.ID)
			return nil
		}
	}

	reply.Type = WAIT
	return nil
}

func (c *Coordinator) Notify(payload *Output, reply *struct{}) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	for id, process := range c.Processes {
		// Ensure the process corresponds to the worker
		if process.TaskID == payload.TaskID && process.WorkerID == payload.WorkerID {

			// Update the process as Done
			process.Done = true
			c.Processes[id] = process

			switch payload.TaskType {
			case MAP:
				log.Println("Coordinator got a map response")
				c.NMapsCompleted++
				if c.NMapsCompleted == c.TotalMapTasks {
					c.CanReduce = true
				}
			case REDUCE:
				log.Println("Coordinator got a reduce response")
				c.Processes[id] = process
				c.NReduceCompleted++
			}
		}
		log.Println("Could not find process ", payload.TaskID)
	}

	return nil
}

func (c *Coordinator) scanAndAssign(workerID string, now time.Time, t TaskType, onAssign func(string, WorkState)) bool {

	for id, st := range c.Processes {
		if st.Type != t || st.Done {
			continue
		}

		if st.WorkerID != "" && now.Sub(st.StartedAt) > 5*time.Second {
			log.Printf("Coordinator - %s task %s timed out (worker %s)n", t.ToString(), id, st.WorkerID)
			st.WorkerID = ""
			st.StartedAt = time.Time{}
			c.Processes[id] = st
		}

		if st.WorkerID == "" {
			st.WorkerID = workerID
			st.StartedAt = now
			c.Processes[id] = st

			onAssign(id, st)

			return true
		}
	}

	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	log.Printf("Coordinator server started!")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	log.Println("Coordinator - Checking for completeness")
	c.mux.Lock()
	defer c.mux.Unlock()

	log.Printf("Maps done: %d/%d, Reduces done: %d/%d\n",
		c.NMapsCompleted, c.TotalMapTasks,
		c.NReduceCompleted, c.NReduce,
	)

	// Both phases must be fully completed
	return c.NMapsCompleted >= c.TotalMapTasks &&
		c.NReduceCompleted >= c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	log.Println("Initializing Coordinator...")
	processes := make(map[string]WorkState)

	// Initialize map processes
	for _, file := range files {
		taskID := uuid.NewString()
		processes[taskID] = WorkState{
			TaskID:    taskID,
			Type:      MAP,
			StartedAt: time.Time{},
			Done:      false,
			FileName:  file,
		}
	}

	// Initialize reduce processes
	for i := range nReduce {
		taskID := uuid.NewString()
		processes[taskID] = WorkState{
			TaskID:    taskID,
			Type:      REDUCE,
			StartedAt: time.Time{},
			Done:      false,
			BucketID:  i,
		}
	}

	c := Coordinator{
		ID:            uuid.NewString(),
		NReduce:       nReduce,
		Processes:     processes,
		TotalMapTasks: len(files),
		mux:           sync.Mutex{},
	}

	c.server()
	return &c
}
