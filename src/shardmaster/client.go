package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labs/src/labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int
	clientId int64
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
	ck.clientId = nrand()
	ck.seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {

	args := &QueryArgs{
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.seq += 1

	// Your code here.
	args.Num = num
	for {
		var reply QueryReply
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Query", args, &reply)
		if ok && reply.Err == OK {
			return reply.Config
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(ClientWaitInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	args := &JoinArgs{
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.seq += 1

	// Your code here.
	args.Servers = servers

	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Join", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(ClientWaitInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {

	args := &LeaveArgs{
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.seq += 1

	// Your code here.
	args.GIDs = gids
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.Err == OK {
			return
		}

		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(ClientWaitInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	args := &MoveArgs{
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	ck.seq += 1

	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		var reply MoveReply
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Move", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(ClientWaitInterval)
	}
}
