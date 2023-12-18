package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	cnt := 0
	for {
		task, filename, nReduce, ok := retrieveTask()
		//fmt.Println("Retrieved Data: " + task + filename + strconv.Itoa(nReduce))

		if task == "map" {
			nTask := ihash(filename)

			intermediate, err := doMap(mapf, filename)
			if err != nil {
				return
			}

			saveMapResults(intermediate, nReduce, nTask)
			ok = callDone("map", filename)
		} else if task == "reduce" {
			index, err := strconv.Atoi(filename)
			if err != nil {
				return
			}

			files := getFiles(index)
			kvData := []KeyValue{}

			for _, filepath := range files {
				file, err := os.Open(filepath)
				if err != nil {
					log.Fatalf("Error while reading file in reduce")
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvData = append(kvData, kv)
				}

			}

			sort.Sort(ByKey(kvData))
			doReduce(reducef, kvData, filename[:1])
			ok = callDone("reduce", filename)
		} else if task == "wait" {
			time.Sleep(time.Millisecond * 100)
		} else {
			return
		}

		if !ok {
			cnt += 1
			if cnt >= 10 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

}

func getFiles(fileIndex int) []string {
	suffix := strconv.Itoa(fileIndex) + ".json"
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error while fetching directory")
		return nil
	}

	files, err2 := os.ReadDir(dir)
	if err2 != nil {
		log.Fatalf("Error while reading directory")
		return nil
	}
	fileSet := []string{}
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), suffix) {
			fileSet = append(fileSet, file.Name())
		}
	}
	//fmt.Println("fileset: ", fileSet)
	return fileSet
}

func doReduce(reducef func(string, []string) string, intermediate []KeyValue, index string) {

	oname := "mr-out-" + index + ".txt"
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return
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
	//intermediate = append(intermediate, kva...)

	return kva, nil
}

func saveMapResults(intermediate []KeyValue, nReduce int, nTask int) bool {

	fileMap := make(map[int]*os.File)
	var err error = nil

	for _, kv := range intermediate {
		nBucket := ihash(kv.Key) % nReduce
		file, exist := fileMap[nBucket]
		if !exist {
			fname := "mr-" + strconv.Itoa(nTask) + "-" + strconv.Itoa(nBucket) + ".json"
			file, err = os.Create(fname)

			if err != nil {
				fmt.Println("error in create")
				log.Fatalf("Error while creating map file for task %d bucket %d", nTask, nBucket)
				return false
			}
			fileMap[nBucket] = file
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Println("error in encode")
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

func retrieveTask() (string, string, int, bool) {
	reply := WorkerTaskReply{}
	args := ExampleArgs{}

	ok := call("Coordinator.ProvideTask", &args, &reply)

	return reply.Task, reply.File, reply.Nreduce, ok
}

func callDone(task string, file string) bool {
	args := WorkerTaskArgs{task, file}
	reply := ExampleReply{}

	ok := call("Coordinator.TaskDone", &args, &reply)

	return ok
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

	return false
}
