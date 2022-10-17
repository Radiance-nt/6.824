package kvraft

import (
	"crypto/rand"
	"labs/src/labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
	clientID int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	ck.seq = 1
	// You'll have to add code here.
	//fmt.Printf("{%d} @ New clerk\n", ck.clientID%1000)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.seq}
	ck.seq += 1
	//DPrintf("{%d} get k: %v", ck.clientID%1000000, key)
	reply := GetReply{}

	// for count := 0; count < len(ck.servers); count++ {
	for {
		reply.Err = ""
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrApplyTimeout {
			//DPrintf("{%d} @ [%d] Seq %v, ok: %v, err: %v ", ck.clientID%1000000, ck.leaderID, args.Seq, ok, reply.Err)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			time.Sleep(ClientWaitInterval)
			continue
		}
		DPrintf("{%d} @ [%d] Get k: %v, v: %v", ck.clientID%1000000, ck.leaderID, key, reply.Value)
		//fmt.Printf("{%d} @ [%d] Get k: %v, v: %v\n", ck.clientID, ck.leaderID, key, reply.Value)
		break
	}
	// You will have to modify this function.
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, Seq: ck.seq}
	ck.seq += 1
	reply := PutAppendReply{}
	//DPrintf("{%d} put k: %v, v: %v", ck.clientID%1000000, key, value)

	// for count := 0; count < len(ck.servers); count++ {
	for {
		reply.Err = ""

		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrApplyTimeout {
			//DPrintf("{%d} @ [%d] Seq %v, ok: %v, err: %v ", ck.clientID%1000000, ck.leaderID, args.Seq, ok, reply.Err)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			time.Sleep(ClientWaitInterval)
			continue
		}
		DPrintf("{%d} @ [%d] Put k: %v, v: %v", ck.clientID%1000000, ck.leaderID, key, value)
		//fmt.Printf("{%d} @ [%d] Put k: %v, v: %v\n", ck.clientID, ck.leaderID, key, value)
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
