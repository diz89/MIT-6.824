package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "sync/atomic"
import "strings"
import "strconv"
import "fmt"

type TaskState int

const (
    Waiting TaskState = iota
    Running
	Finished
)

type Task struct {
	sync.Mutex
	state TaskState
}

func (task *Task) getState() TaskState {
	task.Lock()
	defer task.Unlock()
	return task.state
}

func (task *Task) finish() bool {
	task.Lock()
	defer task.Unlock()
	if task.state != Finished {
		task.state = Finished
		return true
	}
	return false
}

func (task* Task) pick() bool {
	task.Lock()
	defer task.Unlock()
	if task.state == Waiting {
		task.state = Running
		return true
	}
	return false
}

func (task *Task) extend() bool {
	task.Lock()
	defer task.Unlock()
	if task.state != Running {
		return false
	}
	task.state = Waiting
	return true
}

func PickTask(ch chan int, tasks []Task, done chan struct{}) int {
	for {
		select {
			case x := <- ch: 
				if tasks[x].pick() {
					go func() {
						time.Sleep(10 * time.Second)
						if (tasks[x].extend()) {
							ch <- x
						}
					}()
					return x
				}

			case <- done:
				return -1
		}
	}
}

type IntermediateFileNameList struct {
	sync.Mutex
	fileNames []string
}

func (li *IntermediateFileNameList) addFile(fileName string) {
	li.Lock()
	defer li.Unlock()
	li.fileNames = append(li.fileNames, fileName)
}

type Coordinator struct {
	nReduce int
	finishedMapTaskCount uint64
	finishedReduceTaskCount uint64
	mapTasks []Task
	reduceTasks []Task
	inputFiles []string
	mapPhaseDone chan struct{}
	reducePhaseDone chan struct{}
	intermediateFileNameList []IntermediateFileNameList
	mapTaskChan chan int
	reduceTaskChan chan int
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	log.Print("Receive AskTask request: %v\n", args)
	if c.finishedMapTaskCount < uint64(len(c.inputFiles)) {
		x := PickTask(c.mapTaskChan, c.mapTasks, c.mapPhaseDone)
		if x != -1 {
			reply.Op = MapTask
			reply.TaskId = x
			reply.MapTaskParam = MapTaskParam {
				NReduce: c.nReduce,
				FileName: c.inputFiles[x],
			}
			return nil
		}
	}

	if c.finishedReduceTaskCount < uint64(c.nReduce) {
		x := PickTask(c.reduceTaskChan, c.reduceTasks, c.reducePhaseDone)
		if x != -1 {
			reply.Op = ReduceTask
			reply.TaskId = x
			reply.ReduceTaskParam = ReduceTaskParam {
				IntermediateFileNames: c.intermediateFileNameList[x].fileNames,
			}
			return nil			
		}
	}

	reply.Op = Exit
	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	log.Print("Receive FinishMapTask request: %v\n", args)
	x := args.TaskId
	intermediateFilesToAdd := make([][]string, c.nReduce)
	for _, intermediateFileName := range args.IntermediateFileNames {
		reduceTaskId, err := strconv.Atoi(intermediateFileName[strings.LastIndex(intermediateFileName, "-")+1:])
		if err != nil {
			return err
		}
		intermediateFilesToAdd[reduceTaskId] = append(intermediateFilesToAdd[reduceTaskId], intermediateFileName)
	}

	if (c.mapTasks[x].finish()) {
		for i, fileNames := range intermediateFilesToAdd {
			for _, fileName := range fileNames {
				c.intermediateFileNameList[i].addFile(fileName)
			}
		}

		atomic.AddUint64(&(c.finishedMapTaskCount), 1)
		if c.finishedMapTaskCount == uint64(len(c.inputFiles)) {
			close(c.mapPhaseDone) // ?
			go func() {
				for i := 0; i < c.nReduce; i++ {
					c.reduceTaskChan <- i
				}
			}()
		}
		
		return nil
	}
	return nil // TODO

}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	log.Print("Receive FinishReduceTask request: %v\n", args)
	x := args.TaskId
	os.Rename(args.OutFileName, fmt.Sprintf("mr-out-%d", x))
	if (c.reduceTasks[x].finish()) {
		atomic.AddUint64(&(c.finishedReduceTaskCount), 1)
		if c.finishedReduceTaskCount == uint64(c.nReduce) {
			close(c.reducePhaseDone) // ?
		}

		return nil
	}
	return nil // TODO
}


//
// start a thread that listens for RPCs from worker.go
//
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
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.finishedReduceTaskCount == uint64(c.nReduce)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.nReduce = nReduce
	c.mapTasks = make([]Task, len(files))
	c.reduceTasks = make([]Task, nReduce)
	c.mapPhaseDone = make(chan struct{})
	c.reducePhaseDone = make(chan struct{})
	c.intermediateFileNameList = make([]IntermediateFileNameList, nReduce)
	c.mapTaskChan = make(chan int)
	c.reduceTaskChan = make(chan int)

	go func() {
		for i := 0; i < len(c.inputFiles); i++ {
			c.mapTaskChan <- i
		}
	}()

	c.server()
	return &c
}
