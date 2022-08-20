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
	Str1 string
	Str2 string
}
type ReduceStruct struct {
	Str_reduce string
	Result     []string
}

type RequestMissionArgs struct {
	Status   int
	ID       int
	Complete int
	// Channel chan int
}
type RequestMissionReply struct {
	Flag   string // Task type, choice: Map or Reduce
	ID     int
	M_args MapStruct
	R_args ReduceStruct
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
