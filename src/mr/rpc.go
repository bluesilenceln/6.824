package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


// Add your RPC definitions here.
const (
	PhaseMap = 0
	PhaseReduce = 1
)

const (
	TaskMapIdle = 0
	TaskMapBusy = 1
	TaskMapDone = 2

	TaskReduceIdle = 3
	TaskReduceBusy = 4
	TaskReduceDone = 5
)

type TaskMap struct {
	Status int
	MapX int
	File string
}

type TaskReduce struct {
	Status int
	NMap int
	ReduceX int
}

type GetTaskMapArgs struct {

}

type GetTaskMapReply struct {
	Complete bool
	Task *TaskMap
}

type DoneTaskArgs struct {
	Phase int
	Id int
}

type DoneTaskReply struct {

}

type GetTaskReduceArgs struct {

}

type GetTaskReduceReply struct {
	Complete bool
	Task *TaskReduce
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
