package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	files        []string
	nReduce      int
	timeout      int64
	mapFinish    bool
	reduceFinish bool
	mapTasks     []int64
	reduceTasks  []int64
}

type GetFileReply struct {
	Task    string
	TaskId  int
	File    string
	Nreduce int
	Nmap    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetFile(args *ExampleArgs, reply *GetFileReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapFinish { // reduce
		for index, value := range c.reduceTasks {
			now := time.Now().Unix()
			if value == 0 || (value > 0 && now-value >= c.timeout) {
				c.reduceTasks[index] = now
				reply.Task = "reduce"
				reply.TaskId = index
				reply.Nmap = len(c.files)
				reply.Nreduce = c.nReduce
				return nil
			}
		}
	} else {
		for index, value := range c.mapTasks {
			now := time.Now().Unix()
			if value == 0 || (value > 0 && now-value >= c.timeout) {
				c.mapTasks[index] = now
				reply.Task = "map"
				reply.TaskId = index
				reply.File = c.files[index]
				reply.Nmap = len(c.files)
				reply.Nreduce = c.nReduce
				return nil
			}
		}
	}

	reply.Task = "None"

	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == "map" {
		if c.mapTasks[args.TaskId] < 0 {
			return nil
		}
		c.mapTasks[args.TaskId] = -1

		c.mapFinish = true
		for _, value := range c.mapTasks {
			if value != -1 {
				c.mapFinish = false
				break
			}
		}
	} else {
		if c.reduceTasks[args.TaskId] < 0 {
			return nil
		}
		c.reduceTasks[args.TaskId] = -1
		c.reduceFinish = true
		for _, value := range c.reduceTasks {
			if value != -1 {
				c.reduceFinish = false
				break
			}
		}
	}
	return nil
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
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.reduceFinish

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		timeout:      10, // default to be 10 in this lab
		mapFinish:    false,
		reduceFinish: false,
		mapTasks:     make([]int64, len(files)), // 0: task not assigned, -1: finished, > 0: time when assigned
		reduceTasks:  make([]int64, nReduce),
	}

	for index := range files {
		c.mapTasks[index] = 0
		c.reduceTasks[index] = 0
	}


	c.server()
	return &c
}
