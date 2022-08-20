package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	START = iota
	MAP_COMPLETE
	REDUCE_COMPLETE
	EXIT
)

const (
	NOT_DONE = iota
	DOING
	DONE
)

type Pair struct {
	state      int
	start_time time.Time
}
type Master struct {
	// Your definitions here.
	mutex       sync.Mutex
	worker_num  int
	status      int
	counter     int
	task_status [2]map[int]Pair
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) select_task() int {
	var index int
	if m.status < MAP_COMPLETE {
		index = 0
	} else if m.status < REDUCE_COMPLETE {
		index = 1
	} else {
		return -1
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := 0; i < m.worker_num; i++ {
		if m.task_status[index][m.counter].state == NOT_DONE {
			p := Pair{state: DOING, start_time: time.Now()}
			m.task_status[index][m.counter] = p
			return m.counter
		} else {
			m.counter = (m.counter + 1) % m.worker_num
		}
	}
	return -1
}

func (m *Master) AllocateMission(args *RequestMissionArgs, reply *RequestMissionReply) error {
	if args.Complete == MAP_SUCEESS {
		fmt.Printf("[Master] Map %d task complete\n", args.ID)
		if args.ID >= 0 {
			p := Pair{state: DONE, start_time: time.Now()}
			m.task_status[0][args.ID] = p
		}
	} else if args.Complete == REDUCE_SUCEESS {
		fmt.Printf("[Master] Reduce %d task complete\n", reply.ID)
		if args.ID >= 0 {
			p := Pair{state: DONE, start_time: time.Now()}
			m.task_status[1][args.ID] = p
		}
	}
	if args.Status == FREE {
		if m.status == START {
			reply.Flag = "map"
			reply.ID = m.select_task()
			reply.m_args = MapStruct{str1: "1", str2: "2"}
			if reply.ID >= 0 {
				fmt.Printf("[Master] Allocate: %d task start in Map\n", reply.ID)
			}

		} else if m.status == MAP_COMPLETE {
			reply.Flag = "reduce"
			reply.ID = m.select_task()
			if reply.ID >= 0 {
				fmt.Printf("[Master] Allocate: %d task start in Reduce\n", reply.ID)
			}
		} else if m.status == REDUCE_COMPLETE {
			reply.Flag = "exit"
			fmt.Printf("[Master] Allocate: Sending exit signal\n")
		}
	} else {
		fmt.Println("[Master] Allocate: Args.status is not FREE")
		os.Exit(1)
	}
	return nil
}

func (m *Master) init(nReduce int) {
	m.worker_num = nReduce
	m.counter = 0
	m.task_status[0] = make(map[int]Pair)
	m.task_status[1] = make(map[int]Pair)
	for i := 0; i < m.worker_num; i++ {
		p := Pair{state: NOT_DONE, start_time: time.Now()}
		m.task_status[0][i] = p
		m.task_status[1][i] = p
	}
	fmt.Printf("[MakeMaster]: %d workers in total\n", nReduce)

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
		fmt.Println("[Master] Status: REDUCE_COMPLETE")
		ret = true
	}
	return ret
}

func (m *Master) check_task_status() {
	var index, state int
	for {
		if m.status == START {
			index = 0
		} else if m.status == MAP_COMPLETE {
			index = 1
		} else {
			m.status = DONE
			return
		}
		state = 0
		m.mutex.Lock()

		// state := m.task_status[index][0].state
		for i := 0; i < m.worker_num; i++ {
			if m.task_status[index][i].state == DOING && (time.Since(m.task_status[index][i].start_time) > 5*time.Minute) {
				fmt.Printf("[Poll]: Find %d task too long!\n", i)
				p := Pair{state: NOT_DONE, start_time: time.Now()}
				m.task_status[index][i] = p
			}
			if m.task_status[index][i].state != DONE {
				state += 1
			}
		}
		if state == 0 {
			if m.status == START {
				fmt.Printf("[Poll]: All Map Success, index=%d\n\n", index)
			} else if m.status == MAP_COMPLETE {
				fmt.Printf("[Poll]: All Reduce Success, index=%d\n\n", index)
			}
			m.status += 1

		}
		m.mutex.Unlock()

		time.Sleep(time.Second)
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.init(nReduce)
	// Your code here.
	go m.check_task_status()
	m.server()
	return &m
}
