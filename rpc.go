package mr

import (
	"os"
	"strconv"
)

type TaskType int

func (t TaskType) ToString() string {
	switch t {
	case 0:
		return "WAIT"
	case 1:
		return "MAP"
	case 2:
		return "REDUCE"
	default:
		return "EXIT"
	}
}

const (
	WAIT TaskType = iota
	MAP
	REDUCE
	EXIT
)

type TaskRequest struct {
	WorkerID string
}

type Task struct {
	ID            string   // Task identifier
	CoordinatorID string   // CoordinatorID
	Type          TaskType // Task Type

	NReduce  int    // Number of reducers (MAP only)
	FileName string // FileName (MAP only)
	BucketID int    // BucketID (REDUCE only)
}

type Output struct {
	TaskID   string
	WorkerID string
	TaskType TaskType
	BucketID string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
