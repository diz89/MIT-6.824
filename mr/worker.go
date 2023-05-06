package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"
import "os"
import "encoding/json"
import "io/ioutil"
import "time"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	    reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano())
    for {
        args := AskTaskArgs{}
        reply := AskTaskReply{}
        ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			log.Fatal("Call AskTask failed!\n")
		} else {
			log.Print("Receive AskTask reply %v\n", reply)
			switch reply.Op {
                case Exit:
                    fmt.Println("worker exit")
                    return
                case MapTask:
                    runMapTask(mapf, reply.TaskId, reply.MapTaskParam)
                case ReduceTask:
                    runReduceTask(reducef, reply.TaskId, reply.ReduceTaskParam)
            }
        }
    }
}

func runMapTask(mapf func(string, string) []KeyValue, taskId int, mapTaskParam MapTaskParam) {
	file, err := os.Open(mapTaskParam.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapTaskParam.FileName)
		return 
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTaskParam.FileName)
		return 
	}
	file.Close()
	kva := mapf(mapTaskParam.FileName, string(content))

	// output nReduce files
    nReduce := mapTaskParam.NReduce
	encoders := make([]*json.Encoder, nReduce)
	intermediateFileNames := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
        randomFileId := rand.Int()
		intermediateFileName := fmt.Sprintf("mr-worker-tmp-%d-%d", randomFileId, i)
		intermediateFileNames[i] = intermediateFileName
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %s", intermediateFileName)
			return
		}
		defer intermediateFile.Close()
		encoders[i] = json.NewEncoder(intermediateFile)
	}
	// append keys to corresponding buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		// file append write
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv: %v", kv)
			return
		}
	}
	
    args := FinishMapTaskArgs {
        TaskId: taskId,
        IntermediateFileNames: intermediateFileNames,
	}
    reply := FinishMapTaskReply{}
    ok := call("Coordinator.FinishMapTask", &args, &reply)
    if !ok {
        log.Fatal("FinishMapTask call failed!\n")
    }
}

func runReduceTask(reducef func(string, []string) string, taskId int, reduceTaskParam ReduceTaskParam) {
    intermidiateFileNames := reduceTaskParam.IntermediateFileNames
	kva := []KeyValue{}
	for _, intermediateFileName := range intermidiateFileNames {
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
			return
		}
		dec := json.NewDecoder(intermediateFile)
		for  {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
        intermediateFile.Close()
	}
    randomFileId := rand.Int()	
    outFileName := fmt.Sprintf("mr-worker-out-%d", randomFileId)
	fmt.Printf("output file name: %v\n", outFileName)

    outFile, err := os.Create(outFileName)
    if err != nil {
        log.Fatalf("cannot create %s", outFileName)
        return
    }
    defer outFile.Close()

	sort.Sort(ByKey(kva))
	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-tmp-taskID.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		//fmt.Printf("%v %v\n", kva[i].Key, output)

		i = j
	}

    args := FinishReduceTaskArgs {
        TaskId: taskId,
        OutFileName: outFileName,
    }
    reply := FinishReduceTaskReply{}
    ok := call("Coordinator.FinishReduceTask", &args, &reply)
    if !ok {
        log.Fatal("FinishReduceTask call failed!\n")
    }
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
