package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	mfiles  map[string]bool
	rfiles  map[int]bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) provideTask(reply *WorkerTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for file, processed := range c.mfiles {
		if processed == false {
			c.mfiles[file] = true
			reply.file = file
			reply.nreduce = c.nReduce
			reply.task = "map"
			return nil
		}
	}

	for i := 0; i < c.nReduce; i++ {
		if c.rfiles[i] == false {
			c.rfiles[i] = true
			reply.file = strconv.Itoa(i)
			reply.nreduce = c.nReduce
			reply.task = "reduce"
			return nil
		}
	}

	return errors.New("No tasks available")
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		mfiles:  make(map[string]bool),
		rfiles:  make(map[int]bool),
	}

	// Your code here.
	c.mu.Lock()
	for _, file := range files {
		c.mfiles[file] = false
	}
	c.mu.Unlock()

	c.server()
	return &c
}
