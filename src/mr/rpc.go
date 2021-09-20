package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	SLEEP = 0
	FINISH=1
	MAP = 2
	REDUCE = 3
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
//ask for task
type Task struct {
		TaskType int
		TaskNumber int
		Input []string
		StartTime time.Time
}

type RequestTaskArgs struct {
	WorkerName string
	WorkerId int
}

type RequestTaskReturn struct {
	*Task
	NReduce int
}

//report task complete
type ReportTaskArgs struct {
	*Task
	Output []string
}

type ReportTaskReturn struct{}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
