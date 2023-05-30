package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

var (
	TaskType_Map    TaskType = 0
	TaskType_Reduce TaskType = 1
	TaskType_Wait   TaskType = 2
	TaskType_Exit   TaskType = 3
)

type GetTaskReq struct {
}

type GetTaskResp struct {
	TaskType TaskType
	Task     Task
}

type DoneTaskReq struct {
	TaskType TaskType
	TaskId   int
}

type DoneTaskResp struct {
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
