package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	FREE = iota
	MAP_SUCEESS
	MAP_FAILED
	REDUCE_SUCEESS
	REDUCE_FAILED
)

type MapStruct struct {
	str1 string
	str2 string
}
type ReduceStruct struct {
	str_reduce string
	result     []string
}

type RequestMissionArgs struct {
	Status int
	// Channel chan int
}
type RequestMissionReply struct {
	Flag string
	ID   int
	// m_args MapStruct
	// r_args ReduceStruct
}

type CallReply struct {
	flag    string
	Channel chan int
	pair    KeyValue
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
