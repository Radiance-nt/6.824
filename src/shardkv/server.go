package shardkv

import (
	"bytes"
	"labs/src/labrpc"
	"labs/src/shardmaster"
	"sync/atomic"
	"time"
)
import "labs/src/raft"
import "sync"
import "labs/src/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Config   shardmaster.Config
	Shards   map[int]Shard
	ShardNum int
	Op       string
	ClientID int64
	Seq      int64
}

type notifyMsg struct {
	Value string
	Err   Err
}

type notifyKey struct {
	term  int
	index int
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32 // set by Kill()
	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	mck      *shardmaster.Clerk

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // used to read snapshot and raft state size, do not be used to persist

	// Your definitions here.
	groupLeader map[int]int

	// state machine
	stateMachine    map[int]Shard
	config          shardmaster.Config
	shardWaitToPull map[int]targetGroupInfo // key -> shardNum, value -> targetGroup
	shardWaitToOut  map[int]int             // key->shardNum, value -> oldConfigNum
	notifyChMapping map[notifyKey]chan notifyMsg

	MissShardTime int
}

type targetGroupInfo struct {
	TargetGroupID int
	Servers       []string
	OldConfigNum  int
}

type Shard struct {
	ShardID        int
	ConfigNum      int
	Data           map[string]string
	LastSeqMapping map[int64]int64
}

func (kv *ShardKV) getShardByKey(key string) Shard {
	s, ok := kv.stateMachine[key2shard(key)]
	if !ok {
		panic("wrong group when get shard by key")
	}
	return s
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	// DPrintf(" +++ {%d}-{%d} Get %v", args.ClientID, args.Seq, args.Key)

	reply.Value, reply.Err = kv.waitCmdApply(op)
	if reply.Err == OK {
		reply.Value, _ = kv.getShardByKey(args.Key).Data[args.Key]
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	// DPrintf(" ++- {%d}-{%d} Put %v, %v", args.ClientID, args.Seq, args.Key, args.Value)

	_, reply.Err = kv.waitCmdApply(op)
	return
}

func (kv *ShardKV) waitCmdApply(op Op) (string, Err) {
	var result Err
	kv.mu.Lock()
	// check group
	shardNum := key2shard(op.Key)
	result = kv.checkGroup(shardNum)
	if result == ErrWrongGroup || result == ErrApplyTimeout {
		kv.mu.Unlock()
		return "", result
	}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}

	kv.mu.Lock()
	ch := make(chan notifyMsg, 1)
	key := notifyKey{term: term, index: index}
	kv.notifyChMapping[key] = ch
	kv.mu.Unlock()
	var res notifyMsg
	select {
	case res = <-ch:
		kv.mu.Lock()
		//DPrintf("(%d)  --- Res <- ch %v delete op %v", kv.me, res.Value, op)
		delete(kv.notifyChMapping, key)
		result = kv.checkGroup(shardNum)
		if result == ErrWrongGroup || result == ErrApplyTimeout {
			kv.mu.Unlock()
			return "", result
		}
		kv.mu.Unlock()
		return res.Value, res.Err
	case <-time.After(WaitCmdTimeout):
		kv.mu.Lock()
		//DPrintf("(%d)  --- Timeout delete op %v", kv.me, op)
		delete(kv.notifyChMapping, key)
		kv.mu.Unlock()
		return "", ErrApplyTimeout
	}

}

func (kv *ShardKV) checkGroup(shardNum int) Err {

	if kv.config.Shards[shardNum] != kv.gid {
		DPrintf("ErrWrongGroup  shardNum %v gid %v, Shards %v\n", shardNum, kv.gid, kv.config.Shards)
		return ErrWrongGroup
	}
	if _, exist := kv.shardWaitToPull[shardNum]; exist {
		DPrintf("ErrApplyTimeout  shardNum %v gid %v, Shards %v\n", shardNum, kv.gid, kv.shardWaitToPull)

		return ErrApplyTimeout
	}
	return OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applyDeamon() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.Command == nil {
				continue
			}
			kv.mu.Lock()

			if !msg.CommandValid || msg.Command == "Snapshot" {
				kv.readPersist(kv.persister.ReadSnapshot())
				//kv.readPersist(kv.persister.ReadSnapshot())
				kv.mu.Unlock()
				continue
			}
			command := msg.Command.(Op)
			DPrintf("Applied command %v\n", command)
			v := ""
			//fmt.Printf("{%v} op %v, command.Seq %v kv.LastSeqMapping[command.ClientID] %v\n",
			//	command.ClientID, command.Op, command.Seq, kv.LastSeqMapping[command.ClientID])
			switch command.Op {
			case UpdateShard:
				for shardNum, shardValue := range command.Shards {
					if shardValue.ConfigNum > kv.stateMachine[shardNum].ConfigNum {
						DPrintf("enter if execute [UpdateShard], logIndex-%v, shardNum-%v, shard-%v\n",
							msg.CommandIndex, shardNum, command.Shards[shardNum])
						go kv.sendReadySignal(shardNum, kv.shardWaitToPull[shardNum])
						kv.stateMachine[shardNum] = deepCopyShard(shardValue)
						delete(kv.shardWaitToPull, shardNum)
					}
				}
			case ReconfigureOp:
				DPrintf("ReconfigureOp      old %v newConfig %v\n", kv.config, command.Config)
				if command.Config.Num > kv.config.Num {
					kv.Reconfigure(kv.config, command.Config)
					kv.config = deepCopyConfig(command.Config)
					for shardNum, shard := range kv.stateMachine {
						if _, ok := kv.shardWaitToPull[shardNum]; !ok {
							shard.ConfigNum = command.Config.Num
							kv.stateMachine[shardNum] = shard
						}
					}
				}
			case DeleteShard:
				if kv.config.Shards[command.ShardNum] != kv.gid && command.Config.Num >= kv.shardWaitToOut[command.ShardNum] {
					delete(kv.stateMachine, command.ShardNum)
				}
			default:
				// prevent duplicate command here!!!! prevent concurrent reconfig and request
				shardNum := key2shard(command.Key)
				e := kv.checkGroup(shardNum)
				if e == OK && kv.getShardByKey(command.Key).LastSeqMapping[command.ClientID] < command.Seq {
					switch command.Op {
					case GetOp:
						v, _ = kv.getShardByKey(command.Key).Data[command.Key]
					case PutOp:
						kv.stateMachine[shardNum].Data[command.Key] = command.Value
					case AppendOp:
						kv.stateMachine[shardNum].Data[command.Key] += command.Value
					default:
						panic("Error")
					}
					kv.stateMachine[shardNum].LastSeqMapping[command.ClientID] = command.Seq
				}
				term, _ := kv.rf.GetState()
				key := notifyKey{term: term, index: msg.CommandIndex}
				if ch, ok := kv.notifyChMapping[key]; ok {
					ch <- notifyMsg{Value: v, Err: e}
				} else {
				}
			}
			// wake up client-facing RPC handler
			kv.saveSnapshot(msg.CommandIndex)
			kv.mu.Unlock()
		}
	}
}

const rate = 0.9

func (kv *ShardKV) saveSnapshot(appliedId int) {
	// already have kv lock
	if kv.maxraftstate == -1 {
		return
	}
	if float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*rate {
		data := kv.encodeSnapshot()
		go func() { kv.rf.SaveSnapshotWithState(appliedId, data) }()
		//fmt.Printf("{%d} Server snapshot  finish %v > %v\n", kv.me, float32(kv.persister.RaftStateSize()), float32(kv.maxraftstate)*rate)
	}
	return
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.config)
	encoder.Encode(kv.shardWaitToPull)
	encoder.Encode(kv.shardWaitToOut)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var stateMachine map[int]Shard
	var cfg shardmaster.Config
	var shardWaitToPull map[int]targetGroupInfo
	var shardWaitToOut map[int]int
	if d.Decode(&stateMachine) != nil || d.Decode(&cfg) != nil || d.Decode(&shardWaitToPull) != nil ||
		d.Decode(&shardWaitToOut) != nil {
		DPrintf("decode kv state failed on kvserver %v\n\n", kv.me)
	} else {
		kv.stateMachine = stateMachine
		kv.config = cfg
		kv.shardWaitToPull = shardWaitToPull
		kv.shardWaitToOut = shardWaitToOut
	}
}

func (kv *ShardKV) updateConfigUpdate() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if len(kv.shardWaitToPull) == 0 {
				kv.mu.Lock()
				newConfigNum := kv.config.Num + 1
				kv.mu.Unlock()
				newConfig := kv.mck.Query(newConfigNum)
				kv.mu.Lock()
				if kv.config.Num < newConfig.Num {
					DPrintf("find new configuration, old config = %v, new config = %v\n", kv.config, newConfig)
					kv.mu.Unlock()
					command := Op{
						Op:     ReconfigureOp,
						Config: newConfig,
					}
					kv.rf.Start(command)
				} else {
					kv.mu.Unlock()
				}
			}
		}
		time.Sleep(ServerWaitInterval)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.rf.SetGroup(kv.gid) // for test print

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.groupLeader = make(map[int]int)
	kv.shardWaitToPull = make(map[int]targetGroupInfo)
	kv.shardWaitToOut = make(map[int]int)
	kv.notifyChMapping = map[notifyKey]chan notifyMsg{}
	kv.stateMachine = make(map[int]Shard)
	for i := 0; i < len(kv.config.Shards); i++ {
		kv.stateMachine[i] = Shard{
			ShardID:        i,
			ConfigNum:      0,
			Data:           make(map[string]string),
			LastSeqMapping: make(map[int64]int64),
		}
	}
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.applyDeamon()
	go kv.updateConfigUpdate()
	go kv.PullShardsDeamon()

	return kv
}
