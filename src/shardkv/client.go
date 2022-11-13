package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"labs/src/labrpc"
)
import "crypto/rand"
import "math/big"
import "labs/src/shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID      int64
	seq           int64
	groupLeaderID map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.seq = 1
	ck.groupLeaderID = make(map[int]int)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	//ck.seq += 1
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	ck.seq += 1

	reply := GetReply{}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.groupLeaderID[gid]; !ok {
				ck.groupLeaderID[gid] = 0
			}
		Loop:
			for count := 0; count < len(servers); count++ {
				srv := ck.make_end(servers[ck.groupLeaderID[gid]])
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok {
					ck.groupLeaderID[gid] = (ck.groupLeaderID[gid] + 1) % len(servers)
					continue
				}
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrWrongLeader, ErrApplyTimeout:
					ck.groupLeaderID[gid] = (ck.groupLeaderID[gid] + 1) % len(servers)
					continue
				case ErrWrongGroup:
					break Loop
				default:
					panic("Error")
				}
			}
		}
		time.Sleep(ClientWaitInterval)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//ck.seq += 1
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	ck.seq += 1

	reply := PutAppendReply{}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.groupLeaderID[gid]; !ok {
				ck.groupLeaderID[gid] = 0
			}
		Loop:
			for count := 0; count < len(servers); count++ {
				srv := ck.make_end(servers[ck.groupLeaderID[gid]])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok {
					ck.groupLeaderID[gid] = (ck.groupLeaderID[gid] + 1) % len(servers)
					continue
				}
				switch reply.Err {
				case OK:
					return
				case ErrWrongLeader, ErrApplyTimeout:
					ck.groupLeaderID[gid] = (ck.groupLeaderID[gid] + 1) % len(servers)
					continue
				case ErrWrongGroup:
					break Loop
				default:
					panic("Error")
				}

			}
		}
		time.Sleep(ClientWaitInterval)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
