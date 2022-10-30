package shardmaster

import (
	"bytes"
	"labs/src/labgob"
	"labs/src/labrpc"
	"labs/src/raft"
	"log"
	"sync"
	"time"
)

const (
	Debug   = false
	QueryOp = "query"
	JoinOp  = "join"
	LeaveOp = "leave"
	MoveOp  = "move"
)

type notifyMsg struct {
	Value string
	Err   error
}

type notifyKey struct {
	term  int
	index int
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu sync.Mutex
	me int
	rf *raft.Raft

	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	lastSeqMapping  map[int64]int64 // lastSeq[clientID]
	notifyChMapping map[notifyKey]chan notifyMsg

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
}

type Op struct {
	// Your data here.
	Op          string
	ClientID    int64
	Seq         int64
	QueryIndex  int
	ServersJoin map[int][]string
	GidsLeave   []int
	ShardMove   int
	GidMove     int
}

func (sm *ShardMaster) waitCmdApply(op Op) Err {
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	//DPrintf("(%d) wait cmd %v %v clientid %v seq %v\n", kv.me, op.Op, op.Key, op.ClientID, op.Seq)

	sm.mu.Lock()
	ch := make(chan notifyMsg, 1)
	key := notifyKey{term: term, index: index}
	sm.notifyChMapping[key] = ch
	sm.mu.Unlock()
	var res notifyMsg
	select {
	case res = <-ch:
		DPrintf("(%d)  --- Res <- ch %v, op %v", sm.me, res.Value, op)
		sm.mu.Lock()
		//DPrintf("(%d)  --- Res <- ch %v delete op %v", sm.me, res.Value, op)
		delete(sm.notifyChMapping, key)
		sm.mu.Unlock()
		return OK
	case <-time.After(WaitCmdTimeout):
		DPrintf("(%d)  --- Timeout op %v", sm.me, op)

		sm.mu.Lock()
		//DPrintf("(%d)  --- Timeout delete op %v", sm.me, op)
		delete(sm.notifyChMapping, key)
		sm.mu.Unlock()
		return ErrApplyTimeout
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {

		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("--------server[%d] Join Info----------", sm.me)
	for gid, servers := range args.Servers {
		DPrintf("[Gid]==%d\n", gid)
		DPrintf("[servers]%v\n", servers)
	}

	op := Op{
		Op:          JoinOp,
		ClientID:    args.ClientId,
		Seq:         args.Seq,
		ServersJoin: args.Servers,
	}

	result := sm.waitCmdApply(op)
	reply.Err = result
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {

		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Op:        LeaveOp,
		ClientID:  args.ClientId,
		Seq:       args.Seq,
		GidsLeave: args.GIDs,
	}

	result := sm.waitCmdApply(op)
	reply.Err = result

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {

		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Op:        MoveOp,
		ClientID:  args.ClientId,
		Seq:       args.Seq,
		ShardMove: args.Shard,
		GidMove:   args.GID,
	}

	result := sm.waitCmdApply(op)
	reply.Err = result
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {

		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Op:         QueryOp,
		ClientID:   args.ClientId,
		Seq:        args.Seq,
		QueryIndex: args.Num,
	}

	result := sm.waitCmdApply(op)
	reply.Err = result
	if result == OK {
		configIndex := op.QueryIndex
		sm.mu.Lock()
		if configIndex < 0 || configIndex >= len(sm.configs) {
			configIndex = len(sm.configs) - 1
		}
		reply.Config = sm.configs[configIndex]
		//fmt.Printf("config.num %", reply.Config.Num)
		DPrintf("shards:%v", sm.configs[configIndex].Shards)
		sm.mu.Unlock()
		reply.Err = OK

	}
}

func (sm *ShardMaster) applyDeamon() {
	for {
		select {
		case msg := <-sm.applyCh:
			if msg.Command == nil {
				continue
			}
			sm.mu.Lock()

			if !msg.CommandValid || msg.Command == "Snapshot" {
				DPrintf("(%d) |||| ReadPersist", sm.me)
				sm.readPersist(sm.persister.ReadSnapshot())
				sm.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				index := msg.CommandIndex
				// last, _ := sm.lastSeqMapping[op.ClientID]
				term, _ := sm.rf.GetState()
				key := notifyKey{term: term, index: index}

				var v string

				repeat := op.Seq <= sm.lastSeqMapping[op.ClientID]
				repeat = false
				//DPrintf("(%d) ! Index %d, msg client %v, seq %d, sm.lastSeqMapping[op.ClientID] %v", sm.me, index, op.ClientID, op.Seq, sm.lastSeqMapping[op.ClientID])
				//if !repeat {
				sm.lastSeqMapping[op.ClientID] = op.Seq
				//}
				if !repeat {
					//fmt.Printf("config.Num %d\n", config.Num)
					switch op.Op {
					case JoinOp:
						config := sm.getLastConfig()

						for gid, servers := range op.ServersJoin {
							config.Groups[gid] = append(config.Groups[gid], servers...)
						}
						sm.reBalance(&config)
						sm.configs = append(sm.configs, config)

					case LeaveOp:
						config := sm.getLastConfig()

						for _, gid := range op.GidsLeave {

							delete(config.Groups, gid)
							for shardId, oldGid := range config.Shards {
								if oldGid == gid {
									config.Shards[shardId] = 0
								}
							}
						}
						sm.reBalance(&config)
						sm.configs = append(sm.configs, config)

					case MoveOp:
						config := sm.getLastConfig()
						config.Shards[op.ShardMove] = op.GidMove
						sm.configs = append(sm.configs, config)

					case QueryOp:
					default:
						//Err()

					}

				}
				if ch, ok := sm.notifyChMapping[key]; ok {
					DPrintf("(%d) !!!! ch <- Value %v", sm.me, v)
					ch <- notifyMsg{Value: v}
					// DPrintf("(%d) !?? Index %d, msg client %v, seq %d, sm.lastSeqMapping[op.ClientID] %v", sm.me, index, op.ClientID, op.Seq, sm.lastSeqMapping[op.ClientID])
				} else {
					// DPrintf("(%d) !?? Index %d, channel not exist", sm.me, index)
				}
				//DPrintf("(%d) lock to save snap", sm.me)
				sm.saveSnapshot(index)
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastSeq map[int64]int64

	if d.Decode(&persist_config) != nil || d.Decode(&persist_lastSeq) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!", sm.me)
	} else {
		sm.configs = persist_config
		sm.lastSeqMapping = persist_lastSeq
	}
}

func (sm *ShardMaster) encodeSnapshot() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.lastSeqMapping)
	return w.Bytes()
}

const rate = 0.9

func (sm *ShardMaster) saveSnapshot(appliedId int) {
	if sm.maxraftstate == -1 {
		return
	}
	if float32(sm.persister.RaftStateSize()) > float32(sm.maxraftstate)*rate {
		data := sm.encodeSnapshot()
		go func() { sm.rf.SaveSnapshotWithState(appliedId, data) }()
	}
	return
}

func (sm *ShardMaster) getLastConfig() Config {
	config := sm.configs[len(sm.configs)-1]
	config.Num = len(sm.configs)
	config.Groups = make(map[int][]string)
	for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
		config.Groups[gid] = servers
	}
	return config
}

func (sm *ShardMaster) reBalance(config *Config) {
	groupNum := make(map[int][]int) // shard num of group
	var rest []int

	for gid := range config.Groups {
		if gid != 0 {
			groupNum[gid] = []int{}
		}
	}
	for shardId, gid := range config.Shards {
		if gid != 0 {
			groupNum[gid] = append(groupNum[gid], shardId)
		} else {
			rest = append(rest, shardId)
		}
	}
	max := 0x3f3f3f
	DPrintf("before: %v \n", groupNum)
	if len(groupNum) == 0 {
		return
	} else if len(groupNum) == 1 {
		max = NShards
	} else {
		max = (NShards + 1) / len(groupNum)
	}
	for gid, shards := range groupNum {
		if len(shards) > max {
			rest = append(rest, shards[max:]...)
			groupNum[gid] = groupNum[gid][:max]
		}
	}
	DPrintf("phase1: %v \n", groupNum)
	DPrintf("phase1 rest: %v \n", rest)

	for gid, shards := range groupNum {
		if !(len(shards) > max) {
			give := max - len(shards)
			if give > len(rest) {
				give = len(rest)
			}
			DPrintf("gid %d give %v , %v\n", gid, give, rest[:give])

			groupNum[gid] = append(groupNum[gid], rest[:give]...)
			rest = rest[give:]
			if len(rest) == 0 {
				break
			}
		}
	}
	DPrintf("phase2: %v \n", groupNum)
	DPrintf("phase2 rest: %v \n", rest)
	for gid, shards := range groupNum {
		for _, shardId := range shards {
			config.Shards[shardId] = gid
		}
	}
	DPrintf("phase2 config.Shards: %v \n", config.Shards)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.persister = persister
	// Your code here.
	sm.notifyChMapping = map[notifyKey]chan notifyMsg{}
	sm.lastSeqMapping = map[int64]int64{}
	sm.maxraftstate = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sm.mu.Lock()
		sm.readPersist(snapshot)
		sm.mu.Unlock()
	}
	go sm.applyDeamon()

	return sm
}
