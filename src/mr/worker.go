package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		task, complete := callGetMap()
		if complete == true {
			break
		}

		if task == nil {
			//log.Printf("no more task map")
			time.Sleep(1 * time.Second)
			continue
		}

		//log.Printf("get map, file: %s, id: %d", task.File, task.MapX)
		mapPhase(task, mapf)

		callDoneTask(PhaseMap, task.MapX)
	}

	//log.Printf("map tasks complete")

	for {
		task, complete := callGetReduce()
		if complete == true {
			break
		}

		if task == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		//log.Printf("get reduce, id: %d", task.ReduceX)
		reducePhase(task, reducef)

		callDoneTask(PhaseReduce, task.ReduceX)
	}
	//log.Printf("reduce tasks complete")

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func callGetMap() (*TaskMap, bool) {
	args := GetTaskMapArgs{}
	reply := GetTaskMapReply{}

	ret := call("Master.GetTaskMap", &args, &reply)
	if ret == true {
		return reply.Task, reply.Complete
	}

	return nil, false
}

func callGetReduce() (*TaskReduce, bool) {
	args := GetTaskReduceArgs{}
	reply := GetTaskReduceReply{}

	ret := call("Master.GetTaskReduce", &args, &reply)
	if ret == true {
		return reply.Task, reply.Complete
	}

	return nil, false
}

func callDoneTask(phase int, id int) {
	args := DoneTaskArgs{Phase: phase, Id: id}
	reply := DoneTaskReply{}

	ret := call("Master.DoneTask", &args, &reply)
	if ret == true {
	}

	return
}

func mapPhase(task *TaskMap, mapf func(string, string) []KeyValue) {
	nReduce := 10

	file, err := os.Open(task.File)
	if err != nil {
		log.Fatal("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", task.File)
	}
	_ = file.Close()

	midFiles := make(map[int]*os.File)
	midEnc := make(map[int]*json.Encoder)
	for i := 0; i < nReduce; i++ {
		midFile := "mr-" + strconv.Itoa(task.MapX) + "-" + strconv.Itoa(i)
		file, err := os.Create(midFile)
		if err != nil {
			log.Fatal("cannot create %v", midFile)
		}
		midFiles[i] = file
		midEnc[i] = json.NewEncoder(file)
	}

	kva := mapf(task.File, string(content))
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		_ = midEnc[index].Encode(kv)
	}

	for _, f := range midFiles {
		_ = f.Close()
	}
}

func reducePhase(task *TaskReduce, reducef func(string, []string) string) {
	outFile := "mr-out-" + strconv.Itoa(task.ReduceX)
	ofile, err := os.Create(outFile)
	if err != nil {
		log.Fatal("cannot create %v", outFile)
	}

	kva := make([]KeyValue, 0)
	for i := 0; i < task.NMap; i++ {
		midFile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.ReduceX)
		file, err := os.Open(midFile)
		if err != nil {
			log.Fatal("cannot open %v", midFile)
		}
		defer file.Close()

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

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
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
