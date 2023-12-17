package mr

import (
	"errors"
	"fmt"
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
	done    bool
	wg      sync.WaitGroup
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
	fmt.Println("provideTask Called")
	c.mu.Lock()
	defer c.mu.Unlock()
	for file, processed := range c.mfiles {
		if processed == false {
			c.wg.Add(1)
			c.mfiles[file] = true
			reply.File = file
			reply.Nreduce = c.nReduce
			reply.Task = "map"
			fmt.Println("Returning map task")
			return nil
		}
	}

	c.wg.Wait()

	for i := 0; i < c.nReduce; i++ {
		if c.rfiles[i] == false {
			c.wg.Add(1)
			c.rfiles[i] = true
			reply.File = strconv.Itoa(i)
			reply.Nreduce = c.nReduce
			reply.Task = "reduce"
			return nil
		}
	}

	c.wg.Wait()
	c.done = true
	return errors.New("No tasks available")
}

func (c *Coordinator) TaskDone(args *ExampleArgs, reply *ExampleReply) {
	c.mu.Lock()
	defer c.mu.Lock()
	c.wg.Done()
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
		nReduce: nReduce,
		mfiles:  make(map[string]bool),
		rfiles:  make(map[int]bool),
		done:    false,
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
