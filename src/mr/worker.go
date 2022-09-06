package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	for {
		CallGetTask(mapf, reducef)
	}

}

func handleMap(mapf func(string, string) []KeyValue, args *GetTaskReply) {
	file, err := os.Open(args.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", args.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", args.Filename)
	}
	file.Close()
	kva := mapf(args.Filename, string(content))
	intermediate := make([][]KeyValue, args.NReduce)
	for _, v := range kva {
		i := ihash(v.Key) % args.NReduce
		intermediate[i] = append(intermediate[i], v)
	}
	for k, v := range intermediate {
		ofile, _ := ioutil.TempFile("./", "atomic-")
		defer func() {
			ofile.Close()
			os.Remove(ofile.Name())
		}()
		enc := json.NewEncoder(ofile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
		if err := ofile.Close(); err != nil {
			fmt.Println(err.Error())
			return
		}
		os.Rename(ofile.Name(), fmt.Sprintf("mr-%d-%d", args.Taskid, k))
	}
	CallFinishTask(args.Taskid, args.Task)

}
func handleReduce(reducef func(string, []string) string, args *GetTaskReply) {
	kva := []KeyValue{}
	for i := 0; i < args.NMap; i++ {
		file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, args.Taskid))
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	ofile, _ := ioutil.TempFile("./", "atomic-")
	defer func() {
		ofile.Close()
		os.Remove(ofile.Name())
	}()

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

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	if err := ofile.Close(); err != nil {
		fmt.Println(err.Error())
		return
	}
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", args.Taskid))
	CallFinishTask(args.Taskid, args.Task)
}

func CallGetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := GetTaskReply{Task: "None"}
	call("Master.GetTask", &reply, &reply)
	if reply.Task == "None" {
		time.Sleep(time.Second)
	} else if reply.Task == "REDUCE" {
		handleReduce(reducef, &reply)
	} else if reply.Task == "MAP" {
		handleMap(mapf, &reply)
	}
}

func CallFinishTask(taskid int, task string) {
	args := FinishArgs{Taskid: taskid, Task: task}
	call("Master.Finish", &args, &args)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
