package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

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
	sleep_count := 0
	args := RequestMissionArgs{Status: FREE}
	for {
		reply := RequestMission(args)
		if reply.ID == -1 {
			sleep_count += 1
			if sleep_count >= 3 {
				fmt.Printf("[Worker unkonwn]: %d sleep exit\n", sleep_count)
				os.Exit(0)
			}
			fmt.Printf("[Worker unkonwn]: %d sleep\n", sleep_count)
			time.Sleep(time.Duration(5) * time.Second)

			continue
		}
		if reply.Flag == "map" {
			fmt.Printf("[Worker %d]: Map\n", reply.ID)
			inter_kv := mapf(reply.m_args.str1, reply.m_args.str2)
			_ = inter_kv
			args = RequestMissionArgs{Status: FREE, ID: reply.ID, Complete: MAP_SUCEESS}
		} else if reply.Flag == "reduce" {
			fmt.Printf("[Worker %d]: Reduce\n", reply.ID)
			res := mapf(reply.m_args.str1, reply.m_args.str2)
			_ = res
			args = RequestMissionArgs{Status: FREE, ID: reply.ID, Complete: REDUCE_SUCEESS}

		} else {
			fmt.Println("[Worker] ERROR: Reply: ", reply.Flag)
			os.Exit(0)
		}

	}
	// for i := 1; i <= 10; i++ {
	// 	go start_worker()
	// }

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestMission(args RequestMissionArgs) RequestMissionReply {
	reply := RequestMissionReply{}
	err := call("Master.AllocateMission", &args, &reply)
	if !err {
		os.Exit(0)
	}
	return reply
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
