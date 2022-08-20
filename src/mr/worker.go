package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

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
func map_handler(mapf func(string, string) []KeyValue,
	reply RequestMissionReply) {

	inter_name := "mr-inter"
	var file *os.File
	var err error
	intermediate := []KeyValue{}
	filename := reply.M_args.Str1
	file, err = os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(reply.M_args.Str1, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	// TODO: Can be optimized with a buffer
	for _, v := range intermediate {
		filename := fmt.Sprintf("%s-%d", inter_name, ihash(v.Key)%reply.MetaMessage.Reduce_num)
		for {
			if checkFileIsExist(filename) {
				file, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)
			} else {
				file, err = os.Create(filename)
			}
			if err == nil {
				defer file.Close()
				break
			}
			fmt.Printf("[Worker %d]: Open file error, retried..\n", reply.ID)
		}
		content := fmt.Sprintf("%s %s\n", v.Key, v.Value)
		write := bufio.NewWriter(file)
		write.WriteString(content)
		write.Flush()
		// f.WriteString(content)
	}
}

func reduce_handler(reducef func(string, []string) string,
	reply RequestMissionReply) {
	output := reducef(reply.R_args.Str_reduce, reply.R_args.Result)
	_ = output
}

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
			fmt.Printf("[Worker %d]: Map, reply: %v\n", reply.ID, reply)
			map_handler(mapf, reply)
			args = RequestMissionArgs{Status: FREE, ID: reply.ID, Complete: MAP_SUCEESS}
		} else if reply.Flag == "reduce" {
			fmt.Printf("[Worker %d]: Reduce\n", reply.ID)
			reduce_handler(reducef, reply)
			args = RequestMissionArgs{Status: FREE, ID: reply.ID, Complete: REDUCE_SUCEESS}
		} else {
			fmt.Println("[Worker] ERROR: Reply: ", reply.Flag)
			os.Exit(0)
		}

	}
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
