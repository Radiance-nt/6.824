package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	START = iota
	MAPPING
	MAP_COMPLETE
	REDUCING
	REDUCE_COMPLETE
)

type Master struct {
	// Your definitions here.
	mutex       sync.Mutex
	workers_num int
	status      int
	counter     int
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

func (m *Master) AllocateMission(args *RequestMissionArgs, reply *RequestMissionReply) error {
	if args.status == FREE {
		if m.status == START {
			reply.flag = "map"
			reply.id = m.counter
			m.mutex.Lock()
			m.counter += 1
			fmt.Printf("[Master] Allocate: %d task complete in Map", m.counter)
			if m.counter == m.workers_num {
				m.counter = 0
				m.status = MAP_COMPLETE
			}
			m.mutex.Unlock()
		} else if m.status == MAP_COMPLETE {
			reply.flag = "reduce"
			reply.id = m.counter
			m.mutex.Lock()
			m.counter += 1
			fmt.Printf("[Master] Allocate: %d task complete in Reduce", m.counter)
			if m.counter == m.workers_num {
				m.counter = 0
				m.status = REDUCE_COMPLETE
			}
			m.mutex.Unlock()
		}
	} else {
		fmt.Println("[Master] Allocate: Args.status is not FREE")
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
	if m.status == REDUCE_COMPLETE {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.workers_num = nReduce
	m.status = START

	// Your code here.

	m.server()
	return &m
}
