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
	mu         sync.Mutex
	mfiles     map[string]bool
	rfiles     map[int]bool
	mapTask    int
	reduceTask int
	nReduce    int
	done       bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ProvideTask(args *ExampleArgs, reply *WorkerTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for file, processed := range c.mfiles {
		if processed == false {
			c.mfiles[file] = true
			reply.File = file
			reply.Nreduce = c.nReduce
			reply.Task = "map"
			return nil
		}
	}

	if c.mapTask > 0 {
		return errors.New("Wait for other Map workers to finish")
	}

	for i := 0; i < c.nReduce; i++ {
		if c.rfiles[i] == false {
			c.rfiles[i] = true
			reply.File = strconv.Itoa(i)
			reply.Nreduce = c.nReduce
			reply.Task = "reduce"
			return nil
		}
	}

	if c.reduceTask > 0 {
		return errors.New("No tasks available")
	}
	c.done = true
	return errors.New("Tasks completed")
}

func (c *Coordinator) MapTaskDone(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTask -= 1
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTask -= 1
	return nil
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

	// Your code here.

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    nReduce,
		mfiles:     make(map[string]bool),
		rfiles:     make(map[int]bool),
		done:       false,
		mapTask:    len(files),
		reduceTask: nReduce,
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
