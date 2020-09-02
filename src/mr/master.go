package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mtx sync.Mutex
	mapDone int
	reduceDone int

	mapTasks map[int]*TaskMap
	reduceTasks map[int]*TaskReduce

	nMap int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTaskMap(args *GetTaskMapArgs, reply *GetTaskMapReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.mapDone == m.nMap {
		reply.Complete = true
		return nil
	}

	var task *TaskMap
	for _, t := range m.mapTasks {
		if t.Status == TaskMapIdle {
			task = t
			t.Status = TaskMapBusy
			break
		}
	}

	if task != nil {
		reply.Task = task
		//log.Printf("task get ok, file: %s, mapX: %d", task.File, task.MapX)

		go func() {
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			<- timer.C

			m.mtx.Lock()
			defer m.mtx.Unlock()

			if task.Status == TaskMapBusy {
				task.Status = TaskMapIdle
				//log.Printf("task timeout, file: %s, mapX: %d", task.File, task.MapX)
			}
			return
		}()
	}

	return nil
}

func (m *Master) GetTaskReduce(args *GetTaskReduceArgs, reply *GetTaskReduceReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.mapDone < m.nMap {
		return nil
	}

	if m.reduceDone == m.nReduce {
		reply.Complete = true
		return nil
	}

	var task *TaskReduce
	for _, t := range m.reduceTasks {
		if t.Status == TaskReduceIdle {
			task = t
			t.Status = TaskReduceBusy
			break
		}
	}

	if task != nil {
		reply.Task = task
		//log.Printf("task get ok, file: %s, mapX: %d", task.File, task.MapX)

		go func() {
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			<- timer.C

			m.mtx.Lock()
			defer m.mtx.Unlock()

			if task.Status == TaskReduceBusy {
				task.Status = TaskReduceIdle
				//log.Printf("task timeout, file: %s, mapX: %d", task.File, task.MapX)
			}
			return
		}()
	}

	return nil
}

func (m *Master) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if args.Phase == PhaseMap {
		m.mapTasks[args.Id].Status = TaskMapDone
		m.mapDone++
	} else if args.Phase == PhaseReduce {
		m.reduceTasks[args.Id].Status = TaskReduceDone
		m.reduceDone++
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.reduceDone == m.nReduce {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTasks = make(map[int]*TaskMap, 0)
	m.reduceTasks = make(map[int]*TaskReduce, 0)

	for i, f := range files {
		taskM := TaskMap{
			TaskMapIdle,
			i,
			f,
		}
		m.mapTasks[i] = &taskM
	}

	for i := 0; i < m.nReduce; i++ {
		taskR := TaskReduce{
			TaskReduceIdle,
			m.nMap,
			i,
		}
		m.reduceTasks[i] = &taskR
	}

	m.server()
	return &m
}
