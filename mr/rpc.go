package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type Operation int

const (
    MapTask Operation = iota
    ReduceTask
	Exit
)

type AskTaskArgs struct {
	
}

type MapTaskParam struct {
	NReduce int
	FileName string
}

type ReduceTaskParam struct {
	IntermediateFileNames []string
}

type AskTaskReply struct {
	Op Operation
	TaskId int
	MapTaskParam MapTaskParam	
	ReduceTaskParam ReduceTaskParam
}

type FinishMapTaskArgs struct {
	TaskId int
	IntermediateFileNames []string
}

type FinishMapTaskReply struct {

}

type FinishReduceTaskArgs struct {
	TaskId int
	OutFileName string
}

type FinishReduceTaskReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
