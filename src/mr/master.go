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
	files       []string
	nMap        int
	nReduce     int
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
func (m *Master) select_task(reply *RequestMissionReply, task_type string) {
	var task_num int
	var task_ptr *map[int]Pair
	i := 0
	reply.Flag = task_type
	reply.ID = -1
	reply.MetaMessage = Meta{Map_num: m.nMap, Reduce_num: m.nReduce}
	if task_type == "map" {
		task_num = m.nMap
		task_ptr = &m.task_status[0]
	} else if task_type == "reduce" {
		task_num = m.nReduce
		task_ptr = &m.task_status[1]
	} else {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for ; i < task_num; i++ {
		if (*task_ptr)[m.counter].state == NOT_DONE {
			p := Pair{state: DOING, start_time: time.Now()}
			(*task_ptr)[m.counter] = p
			if m.counter >= task_num {
				fmt.Printf("[Master] Serious! task num %d, m.counter %d\n\n",
					task_num, m.counter)
			}
			reply.ID = m.counter
			break
		} else {
			m.counter = (m.counter + 1) % task_num
		}
	}
	if i != task_num {
		if task_type == "map" {
			var filename string
			reply.Flag = "map"
			filename = m.files[reply.ID]
			reply.M_args = MapStruct{Str1: filename, Str2: "2"}
			return
		} else if task_type == "reduce" {
			fmt.Println("First Stage Success")
		}

	}
}

func (m *Master) AllocateMission(args *RequestMissionArgs, reply *RequestMissionReply) error {
	if args.Complete == MAP_SUCEESS {
		fmt.Printf("[Master] Map %d task complete\n", args.ID)
		if args.ID >= 0 {
			p := Pair{state: DONE, start_time: time.Now()}
			m.task_status[0][args.ID] = p
		}
	} else if args.Complete == REDUCE_SUCEESS {
		fmt.Printf("[Master] Reduce %d task complete\n", args.ID)
		if args.ID >= 0 {
			p := Pair{state: DONE, start_time: time.Now()}
			m.task_status[1][args.ID] = p
		}
	}
	if args.Status == FREE {
		if m.status == START {
			m.select_task(reply, "map")
			fmt.Printf("[Master] Allocate: %d task start in Map\n", reply.ID)
		} else if m.status == MAP_COMPLETE {
			m.select_task(reply, "reduce")
			reply.Flag = "reduce"
			fmt.Printf("[Master] Allocate: %d task start in Reduce\n", reply.ID)
		} else {
			m.select_task(reply, "exit")
			fmt.Printf("[Master] Allocate: Sending exit signal\n")
		}
	} else {
		fmt.Println("[Master] Allocate: Args.status is not FREE")
		os.Exit(1)
	}
	return nil
}

func (m *Master) init(files []string, nReduce int) {
	nMap := len(files)
	m.files = files
	m.nMap = nMap
	m.nReduce = nReduce
	m.counter = 0
	m.task_status[0] = make(map[int]Pair)
	m.task_status[1] = make(map[int]Pair)
	for i := 0; i < m.nMap; i++ {
		p := Pair{state: NOT_DONE, start_time: time.Now()}
		m.task_status[0][i] = p
	}
	for i := 0; i < m.nReduce; i++ {
		p := Pair{state: NOT_DONE, start_time: time.Now()}
		m.task_status[0][i] = p
	}
	fmt.Printf("[MakeMaster]: Map num %d; Reduce num %d\n", nMap, nReduce)

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
	var state, task_num int
	var task_ptr *map[int]Pair
	for {
		if m.status == START {
			task_num = m.nMap
			task_ptr = &m.task_status[0]
		} else if m.status == MAP_COMPLETE {
			task_num = m.nReduce
			task_ptr = &m.task_status[1]
		} else {
			m.status = DONE
			return
		}
		state = 0
		m.mutex.Lock()

		// state := m.task_status[index][0].state
		for i := 0; i < task_num; i++ {
			if (*task_ptr)[i].state == DOING && (time.Since((*task_ptr)[i].start_time) > 5*time.Minute) {
				fmt.Printf("[Poll]: Find %d task too long!\n", i)
				p := Pair{state: NOT_DONE, start_time: time.Now()}
				(*task_ptr)[i] = p
			}
			if (*task_ptr)[i].state != DONE {
				state += 1
			}
		}
		if state == 0 {
			if m.status == START {
				fmt.Printf("[Poll]: All Map Success\n\n")
			} else if m.status == MAP_COMPLETE {
				fmt.Printf("[Poll]: All Reduce Success\n\n")
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

	m.init(files, nReduce)
	// Your code here.
	go m.check_task_status()
	m.server()
	return &m
}
