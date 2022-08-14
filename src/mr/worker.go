package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
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
func start_worker() {
	RequestMission()
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// flag := "map"
	// var str1, str2, str_reduce string
	// var strs []string

	// // wait for master
	// if flag == " map" {
	// 	kv_pairs := mapf(str1, str2)
	// 	_ = kv_pairs
	// } else if flag == " reduce" {
	// 	result := reducef(str_reduce, strs)

	// 	// send result back
	// }

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	for i := 1; i <= 10; i++ {
		go start_worker()
	}
	time.Sleep(time.Duration(5) * time.Second)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestMission() {
	args := RequestMissionArgs{Status: FREE}
	reply := RequestMissionReply{}
	call("Master.AllocateMission", &args, &reply)
	fmt.Printf("[Worker] %v\n", reply.ID)
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
