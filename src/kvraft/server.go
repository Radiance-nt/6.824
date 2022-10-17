package kvraft

import (
	"bytes"
	"labs/src/labgob"
	"labs/src/labrpc"
	"labs/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Op       string
	Key      string
	Value    string
	ClientID int64
	Seq      int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type notifyMsg struct {
	Value string
	Err   error
}

type notifyKey struct {
	term  int
	index int
}
type machine struct {
	mapping map[string]string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	stateMachine    machine
	lastSeqMapping  map[int64]int64 // lastSeq[clientID]
	notifyChMapping map[notifyKey]chan notifyMsg

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	// DPrintf(" +++ {%d}-{%d} Get %v", args.ClientID, args.Seq, args.Key)

	reply.Value, reply.Err = kv.waitCmdApply(op)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *KVServer) waitCmdApply(op Op) (string, Err) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}
	DPrintf("(%d) wait cmd %v %v clientid %v seq %v\n", kv.me, op.Op, op.Key, op.ClientID, op.Seq)

	kv.mu.Lock()
	ch := make(chan notifyMsg, 1)
	key := notifyKey{term: term, index: index}
	kv.notifyChMapping[key] = ch
	kv.mu.Unlock()
	var res notifyMsg
	select {
	case res = <-ch:
		DPrintf("(%d)  --- Res <- ch %v, op %v", kv.me, res.Value, op)
		kv.mu.Lock()
		//DPrintf("(%d)  --- Res <- ch %v delete op %v", kv.me, res.Value, op)
		delete(kv.notifyChMapping, key)
		kv.mu.Unlock()
		return res.Value, ""
	case <-time.After(WaitCmdTimeout):
		DPrintf("(%d)  --- Timeout op %v", kv.me, op)

		kv.mu.Lock()
		//DPrintf("(%d)  --- Timeout delete op %v", kv.me, op)
		delete(kv.notifyChMapping, key)
		kv.mu.Unlock()
		return "", ErrApplyTimeout
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyDeamon() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.Command == nil {
				continue
			}
			kv.mu.Lock()

			if !msg.CommandValid || msg.Command == "Snapshot" {
				DPrintf("(%d) |||| ReadPersist", kv.me)
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.mu.Unlock()

			} else {
				op := msg.Command.(Op)
				index := msg.CommandIndex
				// last, _ := kv.lastSeqMapping[op.ClientID]
				term, _ := kv.rf.GetState()
				key := notifyKey{term: term, index: index}

				var v string

				repeat := op.Seq <= kv.lastSeqMapping[op.ClientID]
				//DPrintf("(%d) ! Index %d, msg client %v, seq %d, kv.lastSeqMapping[op.ClientID] %v", kv.me, index, op.ClientID, op.Seq, kv.lastSeqMapping[op.ClientID])
				//if !repeat {
				kv.lastSeqMapping[op.ClientID] = op.Seq
				//}

				switch op.Op {

				case "Put":
					if !repeat {
						kv.stateMachine.mapping[op.Key] = op.Value
						//DPrintf("(%d) !!!Apply Put[%v] = %v --- ck %v, seq %v", kv.me, op.Key, op.Value, op.ClientID, op.Seq)
					}
				case "Append":
					if !repeat {
						kv.stateMachine.mapping[op.Key] += op.Value
						v = kv.stateMachine.mapping[op.Key]
						//DPrintf("(%d) !!!Apply App[%v] = %v --- ck %v, seq %v", kv.me, op.Key, v, op.ClientID, op.Seq)
					}
				case "Get":
					v = kv.stateMachine.mapping[op.Key]
					// if !exist {
					// 	v = ""
					// }
					//DPrintf("(%d) !!!Apply Get[%v] = %v", kv.me, op.Key, v)

				default:
					DPrintf("(%d) ~~~~~~FATAL~~~~~~~~~~~~~ %v", kv.me, op.Op)

				}
				if ch, ok := kv.notifyChMapping[key]; ok {
					DPrintf("(%d) !!!! ch <- Value %v", kv.me, v)
					ch <- notifyMsg{Value: v}
					// DPrintf("(%d) !?? Index %d, msg client %v, seq %d, kv.lastSeqMapping[op.ClientID] %v", kv.me, index, op.ClientID, op.Seq, kv.lastSeqMapping[op.ClientID])
				} else {
					// DPrintf("(%d) !?? Index %d, channel not exist", kv.me, index)
				}
				//DPrintf("(%d) lock to save snap", kv.me)
				kv.saveSnapshot(index)
				kv.mu.Unlock()
				//DPrintf("(%d) unlock to save snap", kv.me)

			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.stateMachine = machine{mapping: make(map[string]string)}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.notifyChMapping = map[notifyKey]chan notifyMsg{}
	kv.lastSeqMapping = map[int64]int64{}
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyDeamon()
	return kv
}

const rate = 0.9

func (kv *KVServer) saveSnapshot(appliedId int) {
	if kv.maxraftstate == -1 {
		return
	}
	if float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*rate {
		data := kv.encodeSnapshot()
		kv.rf.SaveSnapshotWithState(appliedId, data)
		//fmt.Printf("{%d} Server snapshot  finish %v > %v\n", kv.me, float32(kv.persister.RaftStateSize()), float32(kv.maxraftstate)*rate)

	}
	return
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine.mapping)
	e.Encode(kv.lastSeqMapping)
	return w.Bytes()
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var lastSeq map[int64]int64

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastSeq) != nil {
		DPrintf("{%d} FATAL, readPersist failed", kv.me)
	} else {
		kv.stateMachine.mapping = kvData
		kv.lastSeqMapping = lastSeq
	}
}
