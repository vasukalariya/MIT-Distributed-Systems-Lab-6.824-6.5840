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
	taskId string
	status   string
	lastTime time.Time
	mu sync.Mutex
}

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	mfiles     map[string]*Task
	rfiles     map[string]*Task
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
	
	if c.mapTask > 0 {

		for file, _ := range c.mfiles {
			if c.mfiles[file].status == "left" {
				c.mfiles[file].status = "running map"
				c.mfiles[file].lastTime = time.Now()
				reply.File = file
				reply.Nreduce = c.nReduce
				reply.Task = "map"

				c.mu.Unlock()
				go waitForTask(c.mfiles[file])

				return nil
			}
		}	

		reply.Task = "wait"
		c.mu.Unlock()
		return nil
	}

	if c.reduceTask > 0 {

		for i := 0; i < c.nReduce; i++ {
			idx := strconv.Itoa(i)
			if c.rfiles[idx].status == "left" {
				c.rfiles[idx].status = "running reduce"
				c.rfiles[idx].lastTime = time.Now()
				reply.File = idx
				reply.Nreduce = c.nReduce
				reply.Task = "reduce"
				
				c.mu.Unlock()
				go waitForTask(c.rfiles[idx])

				return nil
			}
		}

		reply.Task = "wait"
		c.mu.Unlock()
		return nil
	}

	reply.Task = "exit"
	c.mu.Unlock()
	return nil
}

func waitForTask(task *Task) {
	time.Sleep(time.Second*10)
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.status != "completed" {
		task.status = "left"
		task.lastTime = time.Now()
	}
}

func (c *Coordinator) TaskDone(args *WorkerTaskArgs, reply *WorkerDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if args.Task == "map" {
		c.mfiles[args.File].status = "completed"
		c.mfiles[args.File].lastTime = time.Now()
		c.mapTask -= 1
	} else {
		c.rfiles[args.File].status = "completed"
		c.rfiles[args.File].lastTime = time.Now()
		c.reduceTask -= 1
	}
	
	reply.Exit = (c.mapTask == 0 && c.reduceTask == 0)
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
	
	return c.mapTask == 0 && c.reduceTask == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    nReduce,
		mfiles:     make(map[string]*Task),
		rfiles:     make(map[string]*Task),
		mapTask:    len(files),
		reduceTask: nReduce,
	}

	// Your code here.
	c.mu.Lock()
	for _, file := range files {
		c.mfiles[file] = &Task{file, "left", time.Now(), sync.Mutex{}}
	}
	for i := 0; i < nReduce; i++ {
		idx := strconv.Itoa(i)
		c.rfiles[idx] = &Task{idx, "left", time.Now(), sync.Mutex{}}
	}
	c.mu.Unlock()

	c.server()
	return &c
}
