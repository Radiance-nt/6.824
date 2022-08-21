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

var inter_name = "mr-inter"
var out_name = "mr-out"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func open_file(filename string) (*os.File, error) {
	var file *os.File
	var err error
	if checkFileIsExist(filename) {
		file, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)
	} else {
		file, err = os.Create(filename)
	}

	return file, err
}

//
// main/mrworker.go calls this function.
//
func map_handler(mapf func(string, string) []KeyValue,
	reply RequestMissionReply) {

	var file *os.File
	var err error
	nReduce := reply.MetaMessage.Reduce_num
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

	jsonEncoder := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("%s-%d-%d", inter_name, reply.ID, i)
		f, err := open_file(filename)
		if err != nil {
			fmt.Printf("[Worker %d]: Create json file error\n", reply.ID)
			return
		}
		jsonEncoder[i] = json.NewEncoder(f)
		defer f.Close()
	}

	// TODO: Can be optimized with a buffer
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		err := jsonEncoder[index].Encode(&kv)
		if err != nil {
			fmt.Printf("[Worker %d]: Json to %d failed\n", reply.ID, index)
			return
		}
	}
}

func reduce_handler(reducef func(string, []string) string,
	reply RequestMissionReply) {
	nMap := reply.MetaMessage.Map_num
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("%s-%d-%d", inter_name, i, reply.ID)
		file, err := os.OpenFile(filename, os.O_RDONLY, 0)
		if err != nil {
			fmt.Printf("[Worker %d] Reduce: Open file error %v\n", reply.ID, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// fmt.Printf("[Worker %d] Reduce: kv %v\n", reply.ID, err)
			intermediate = append(intermediate, kv)
		}
	}
	// fmt.Printf("[Worker %d] Reduce output: %v \n", reply.ID, intermediate)
	sort.Sort(ByKey(intermediate))

	// filename := fmt.Sprintf("%s-%d", out_name, reply.ID)
	filename := fmt.Sprintf("%s-%d", out_name, 0)
	ofile, _ := open_file(filename)
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		// fmt.Printf("[Worker %d] Reduce output: %v \n", reply.ID, output)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	sleep_count := 0
	args := RequestMissionArgs{Status: FREE}
	for {
		reply := RequestMission(args)
		if reply.ID == -1 {
			sleep_count += 1
			if sleep_count >= 5 {
				fmt.Printf("[Worker unkonwn]: %d sleep exit\n", sleep_count)
				os.Exit(0)
			}
			fmt.Printf("[Worker unkonwn]: %d sleep\n", sleep_count)
			time.Sleep(time.Duration(3*sleep_count) * time.Second)

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
