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

type Master struct {
	// Your definitions here.
	lock        sync.Mutex
	mapFinish   bool
	finish      bool
	files       []string
	mapTasks    []int64
	reduceTasks []int64
	timeout     int64
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Get a task from master; prioritize map tasks
func (m *Master) GetTask(_ *GetTaskReply, reply *GetTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	timeNow := time.Now().Unix()
	if m.mapFinish { // assign a reduce task
		for k, v := range m.reduceTasks {
			if v == 0 || (v > 0 && timeNow-v > m.timeout) {
				reply.Taskid = k
				m.reduceTasks[k] = timeNow
				reply.Task = "REDUCE"
				reply.NMap = len(m.files)
				break
			}
		}
	} else { // assign a map task
		for k, v := range m.mapTasks {
			if v == 0 || (v > 0 && timeNow-v > m.timeout) {
				reply.Taskid = k
				m.mapTasks[k] = timeNow
				reply.Filename = m.files[k]
				reply.Task = "MAP"
				reply.NReduce = m.nReduce
				break
			}
		}
	}
	return nil
}

func (m *Master) Finish(args *FinishArgs, _ *FinishArgs) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.Task == "MAP" {
		m.mapTasks[args.Taskid] = -1
		m.mapFinish = true
		for _, value := range m.mapTasks {
			if value != -1 {
				m.mapFinish = false
				break
			}
		}
	} else {
		m.reduceTasks[args.Taskid] = -1
		m.finish = true
		for _, value := range m.reduceTasks {
			if value != -1 {
				m.finish = false
				break
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.lock.Lock()
	defer m.lock.Unlock()
	ret = m.finish

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		timeout:     10,
		mapFinish:   false,
		finish:      false,
		mapTasks:    make([]int64, len(files)),
		reduceTasks: make([]int64, nReduce),
	}
	for i := range files {
		m.mapTasks[i] = 0
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = 0
	}

	m.server()
	return &m
}
