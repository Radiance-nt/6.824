package shardkv

import (
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrApplyTimeout    = "ErrApplyTimeout"
	ClientWaitInterval = 20 * time.Millisecond
	ServerWaitInterval = 100 * time.Millisecond
	WaitCmdTimeout     = 2 * time.Second
)

const (
	ReconfigureOp = "Reconfigure"
	DeleteShard   = "DeleteShard"
	UpdateShard   = "UpdateShard"
	GetOp         = "Get"
	PutOp         = "Put"
	AppendOp      = "Append"
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

type PullShardArgs struct {
	ConfigNum int
	ShardNum  int
}

type PullShardReply struct {
	Err
	ConfigNum        int
	ShardData        map[string]string
	ClientRequestSeq map[int64]int64
}

type SendReadyArgs struct {
	ConfigNum int
	ShardNum  int
}

type SendReadyReply struct {
	Err
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
