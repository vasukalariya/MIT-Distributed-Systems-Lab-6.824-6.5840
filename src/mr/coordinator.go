package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	status   string
	lastTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	mfiles     map[string]Task
	rfiles     map[int]Task
	mapTask    int
	reduceTask int
	nReduce    int
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
	for file, task := range c.mfiles {
		if task.status == "left" {
			c.mfiles[file] = Task{"running map", time.Now()}
			reply.File = file
			reply.Nreduce = c.nReduce
			reply.Task = "map"
			return nil
		}
	}

	if c.mapTask > 0 {
		for file, task := range c.mfiles {
			if task.status == "running map" {
				cur := time.Now()
				diff := cur.Sub(task.lastTime)
				if diff.Seconds() > 5 {
					c.mfiles[file] = Task{"left", time.Now()}
				}
			}
		}
		reply.Task = "wait"
		return nil
	}

	for i := 0; i < c.nReduce; i++ {
		if c.rfiles[i].status == "left" {
			c.rfiles[i] = Task{"running reduce", time.Now()}
			reply.File = strconv.Itoa(i)
			reply.Nreduce = c.nReduce
			reply.Task = "reduce"
			return nil
		}
	}

	if c.reduceTask > 0 {
		for i := 0; i < c.nReduce; i++ {
			if c.rfiles[i].status == "running map" {
				cur := time.Now()
				diff := cur.Sub(c.rfiles[i].lastTime)
				if diff.Seconds() > 5 {
					c.rfiles[i] = Task{"left", time.Now()}
				}
			}
		}
		reply.Task = "wait"
		return nil
	}

	reply.Task = "exit"
	return nil
}

func (c *Coordinator) TaskDone(args *WorkerTaskArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == "map" {
		c.mfiles[args.File] = Task{"completed", time.Now()}
		c.mapTask -= 1
	} else {
		filenum, _ := strconv.Atoi(args.File)
		c.rfiles[filenum] = Task{"completed", time.Now()}
		c.reduceTask -= 1
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceTask == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    nReduce,
		mfiles:     make(map[string]Task),
		rfiles:     make(map[int]Task),
		mapTask:    len(files),
		reduceTask: nReduce,
	}

	// Your code here.
	c.mu.Lock()
	for _, file := range files {
		c.mfiles[file] = Task{"left", time.Now()}
	}
	for i := 0; i < nReduce; i++ {
		c.rfiles[i] = Task{"left", time.Now()}
	}
	c.mu.Unlock()

	c.server()
	return &c
}
