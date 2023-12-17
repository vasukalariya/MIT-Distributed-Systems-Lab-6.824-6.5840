package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	task, filename, nReduce := retrieveTask()

	if task == "map" {
		nTask := ihash(filename) % nReduce

		intermediate, err := doMap(mapf, filename)
		if err == nil {
			return
		}
		saveMapResults(intermediate, nReduce, nTask)
	} else if task == "reduce" {

	} else {
		log.Fatalf("Invalid task passed")
	}

}

func doMap(mapf func(string, string) []KeyValue, filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	intermediate := []KeyValue{}
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return intermediate, errors.New("Cannot open file" + filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return intermediate, errors.New("Cannot read file" + filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	return intermediate, nil
}

func saveMapResults(intermediate []KeyValue, nReduce int, nTask int) bool {

	fileMap := make(map[int]*os.File)
	var err error

	for _, kv := range intermediate {
		nBucket := ihash(kv.Key) % nReduce
		file, exist := fileMap[nBucket]
		if !exist {
			fname := "mr-" + strconv.Itoa(nTask) + "-" + strconv.Itoa(nBucket) + ".json"
			file, err = os.Create(fname)
			if err != nil {
				log.Fatalf("Error while creating map file for task %d bucket %d", nTask, nBucket)
				return false
			}
			fileMap[nBucket] = file
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Println("Error encoding JSON:", err)
			return false
		}
	}

	for _, f := range fileMap {
		f.Close()
	}

	return true
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func retrieveTask() (string, string, int) {
	reply := WorkerTaskReply{}

	ok := call("Coordinator.provideMapTask", nil, &reply)
	if ok {
		return reply.task, reply.file, reply.nreduce
	}
	return "", "", 0
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
