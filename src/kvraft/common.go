package kvraft

import (
	"log"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrApplyTimeout = "ErrApplyTimeout"
	// WaitCmdTimeout  = 50 * time.Millisecond
	WaitCmdTimeout = 2 * time.Second
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID int64
	Seq      int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
