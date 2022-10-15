package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"errors"
	"labs/src/labgob"
	"labs/src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labs/src/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	STATE_FOLLOWER = iota
	STATE_CANDIDATE
	STATE_LEADER
)

type entry struct {
	Term    int
	Message interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	electionTimer   *time.Ticker
	heartbeatTimer  *time.Ticker
	leader_cond     *sync.Cond
	replicator_cond []*sync.Cond
	state           int

	// persistent state
	currentTerm int
	votedFor    int
	log         []entry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	if rf.state == STATE_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	if rf.killed() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("[%d] @ Save Persist %d, %d, %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	// DPrintf("[%d] @@@ Read Persist\n", rf.me)
	// DPrintf("[%d] @@@ Read Persist %v\n", rf.me, data)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic(errors.New("Error Reading Persitence"))

	} else {

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		DPrintf("[%d] @@@ Read Persist,%d %d  %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)

	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type RequestAppendEntriesArgs struct {
	// Your data here (2A, 2B)..
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type RequestAppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term          int
	Success       bool
	ConflictIndex int
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// DPrintf("[%d] ***AE get lock\n", rf.me)
	defer rf.mu.Unlock()
	// defer DPrintf("[%d] ***AE unlock\n", rf.me)
	defer rf.persist()
	// defer DPrintf("[%d] ** defer appendEntrie  persist\n", rf.me)

	if args.Term < rf.currentTerm {
		if len(args.Entries) == 0 {
			// DPrintf("[%d] Recieved HB: args.Term %d < rf.currentTerm %d from %d\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)

		} else {
			DPrintf("[%d] *** Recieved Appending, args.Term %d < rf.currentTerm %d from %d\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE) {
		if rf.state == STATE_LEADER {
			DPrintf("[%d] Heard HB, From Leader to follower, args.Term %d < rf.currentTerm %d from %d\n",
				rf.me, args.Term, rf.currentTerm, args.LeaderId)
		} else if args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE {
			DPrintf("[%d] Heard HB, From Candidate to follower, args.Term %d < rf.currentTerm %d from %d\n",
				rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		rf.currentTerm = args.Term
		rf.ChangeState(STATE_FOLLOWER)
	}
	DPrintf("[%d] Heard HB from %d of %d term, reset the timer \n", rf.me, args.LeaderId, args.Term)

	rf.ResetelectionTimer()

	// return false if log doesn't contain an entry matching prevLogIndex whose term matches preLogTerm, follow rule no.2
	DPrintf("[%d] ***  Heard appending from %d of term %d, len(rf.log) %d, PrevLogIndex=%d \n",
		rf.me, args.LeaderId, args.Term, len(rf.log), args.PrevLogIndex)

	if len(rf.log) <= args.PrevLogIndex {
		// DPrintf("[%d] ***  Respond failed, len(rf.log) %d \n", rf.me, len(rf.log))

		reply.ConflictIndex = len(rf.log)
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] ***  Respond failed, args.PrevLogIndex %d \n", rf.me, args.PrevLogIndex)
		reply.ConflictIndex = 1
		conflictTerm := rf.log[args.PrevLogIndex].Term
		DPrintf("[%d] *** len(rf.log)=%d, rf.log[len(rf.log) - 1].Term = %d\n", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)

		for i := len(rf.log) - 1; i > 0; i-- {
			if rf.log[i].Term != conflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		DPrintf("[%d] ***  ConflictIndex set to %d, rf.log[args.PrevLogIndex].Term = %d, rf.log[i-1:].Term=%d\n",
			rf.me, reply.ConflictIndex, rf.log[args.PrevLogIndex].Term, rf.log[reply.ConflictIndex-1:])
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}
	DPrintf("[%d] ***  Respond appending success to %d, PrevLogIndex=%d \n", rf.me, args.LeaderId, args.PrevLogIndex)

	reply.Success = true
	reply.Term = rf.currentTerm

	next_log := args.PrevLogIndex + 1
	// delete its own log and follow the leader
	if len(rf.log) > next_log && rf.log[next_log].Term != args.Term {
		// DPrintf("[%d] *** Delete rf.log = rf.log[:next_log %d] ,rf.log[next_log-1:] is %v, and len(rf.log) is %d \n", rf.me, next_log, rf.log[next_log-1:], len(rf.log))
		rf.log = rf.log[:next_log]
		DPrintf("[%d] *** and Now len(rf.log) %d \n", rf.me, len(rf.log))
	}
	DPrintf("[%d] **** Leader%d commit=%d, rf.commitIndex=%d, lastApplied=%d\n",
		rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, rf.lastApplied)

	if len(args.Entries) > 0 {
		var entries []entry
		if len(rf.log)-next_log <= len(args.Entries) {
			entries = args.Entries[len(rf.log)-next_log:]

		} else {
			reply.ConflictIndex = len(rf.log)
		}
		rf.log = append(rf.log, entries...)
	}
	DPrintf("[%d] **** Appending log %v done, and len(rf.log) is %d, log is %v \n", rf.me, args.Entries, len(rf.log), rf.log)

	rf.update_commit(*args)

}
func (rf *Raft) update_commit(args RequestAppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		DPrintf("[%d] **** rf.commitIndex = min(args.LeaderCommit %d, rf.getLastIndex() %d)\n",
			rf.me, args.LeaderCommit, rf.getLastIndex())
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendHeartBeat(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) replicator(i int) {
	rf.replicator_cond[i].L.Lock()
	defer rf.replicator_cond[i].L.Unlock()
	for !rf.killed() {
		// DPrintf("[%d] ** Replicator is waiting  \n", rf.me)

		rf.replicator_cond[i].Wait()
		term := rf.currentTerm

		// DPrintf("[%d] ** Replicator recieves signal  \n", rf.me)
		for !rf.killed() && rf.currentTerm == term && rf.state == STATE_LEADER {
			DPrintf("[%d] ** Send append entries to %d  \n", rf.me, i)

			rf.sendHB(i, false)

			rf.mu.Lock()
			// DPrintf("[%d] ** replicator get lock %d  \n", rf.me, i)

			if !(rf.currentTerm == term && rf.state == STATE_LEADER) {

				rf.mu.Unlock()
				break
			}
			if rf.getLastIndex() >= rf.nextIndex[i] {
				DPrintf("[%d] ** Update %d log, prevLogIndex= %d \n", rf.me, i, rf.nextIndex[i]-1)
				// DPrintf("[%d] ** replicator unlock %d  \n", rf.me, i)

				rf.mu.Unlock()

			} else {
				DPrintf("[%d] ## Machine %d up to date, send normal hb, len(entries)=%d\n",
					rf.me, i, len(rf.log[rf.nextIndex[i]:]))
				// DPrintf("[%d] ** replicator unlock %d  \n", rf.me, i)

				rf.mu.Unlock()

				break
			}
		}
	}
}

func (rf *Raft) sendHB(i int, heartbeat bool) {
	var args RequestAppendEntriesArgs
	var prevLogIndex int
	var prevLogTerm int
	// DPrintf("[%d] #* send did not get lock\n", rf.me)

	rf.mu.Lock()
	if rf.state != STATE_LEADER {
		rf.mu.Unlock()
		return
	}
	// TODO: Introduce a fatal bug
	DPrintf("[%d] #* Send hb to %d, prevLogIndex= %d, len(rf.log) %d\n",
		rf.me, i, rf.nextIndex[i]-1, len(rf.log))
	prevLogIndex = rf.nextIndex[i] - 1
	prevLogTerm = rf.getIndexTerm(prevLogIndex)

	DPrintf("[%d] #* Send normal hb to %d, prevLogIndex= %d, len(rf.log) %d, Entries %v\n",
		rf.me, i, prevLogIndex, len(rf.log), rf.log[prevLogIndex+1:])
	args = RequestAppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.log[prevLogIndex+1:],
		PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}

	reply := RequestAppendEntriesReply{}

	rf.mu.Unlock()
	// DPrintf("[%d] #* send HB unlock\n", rf.me)

	ok := rf.sendHeartBeat(i, &args, &reply)
	if !ok {
		DPrintf("[%d] #* sendHeartBeat not ok from %d \n", rf.me, i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Does it need to lock on and check if rf.currentTerm > term?
	if reply.Term > rf.currentTerm {
		DPrintf("[%d] ** Turn to follower, recieved from machine%d, reply.Term %d > rf.currentTerm %d\n",
			rf.me, i, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.ChangeState(STATE_FOLLOWER)
		DPrintf("[%d] ** Turn to follower, when sendHB  persist\n", rf.me)
		rf.persist()
		return
	}
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Success {
		if rf.matchIndex[i] > prevLogIndex {
			return
		}
		rf.matchIndex[i] = prevLogIndex
		rf.nextIndex[i] = max(prevLogIndex+1+len(args.Entries), reply.ConflictIndex)

		if len(args.Entries) > 0 {
			DPrintf("[%d] ** Machine%d reply success, matchIndex=%d, nextIndex=%d, len(args.Entries)=%d \n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], len(args.Entries))
		}
	} else {
		if rf.nextIndex[i] < 1 {
			DPrintf("[%d] ** FATAL: rf.nextIndex[i] < 0, args:%v, reply: %v \n",
				rf.me, args, reply)
		}
		rf.nextIndex[i] = reply.ConflictIndex
		// rf.nextIndex[i] = prevLogIndex
		DPrintf("[%d] ** Failed to update %d log, rf.nextIndex set to ConflictIndex %d \n", rf.me, i, rf.nextIndex[i])
	}
	rf.persist()
	DPrintf("[%d] ** success when sendHB  persist\n", rf.me)

}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getIndexTerm(index int) int {
	if index < 0 {
		return 0
	}
	return rf.log[index].Term
}

func (rf *Raft) findLargestcommitIndex() int {
	n := rf.commitIndex
	last := n
	if rf.state != STATE_LEADER {
		return last
	}
	for !rf.killed() {
		c := 0
		for i := range rf.peers {
			if i != rf.me {
				if rf.matchIndex[i] >= n {
					c += 1
				}
			}
		}
		if !(n < len(rf.log)) {

			break
		} else if rf.log[n].Term > rf.currentTerm {
			DPrintf("[%d] * rf.log[n].Term %d> rf.currentTerm %d\n", rf.me, rf.log[n].Term, rf.currentTerm)

			break
		} else if rf.log[n].Term == rf.currentTerm && (float64(c+1) >= float64(len(rf.peers))/2) {
			last = n
			DPrintf("[%d] * Majority sent of %d, matchindex=%v, c=%d\n", rf.me, last, rf.matchIndex, c)

		} else if rf.log[n].Term < rf.currentTerm {
			n += 1
			continue
		} else {
			break
		}
		n += 1
	}
	DPrintf("[%d] * The largest commitIndex set to %d, current term %d\n", rf.me, last, rf.currentTerm)
	return last
}

func (rf *Raft) handle(command interface{}) {
	DPrintf("[%d] * Handle client's message %v, log is %v, my last is %d\n", rf.me, command, rf.log, rf.getLastIndex())

	for i := range rf.peers {
		if i != rf.me {
			rf.replicator_cond[i].Signal()
		}
	}
	time.Sleep(heartbeat_timeout)
	rf.mu.Lock()
	rf.commitIndex = rf.findLargestcommitIndex()

	rf.mu.Unlock()

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
const apply_detect_interval = 20 * time.Millisecond

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		commitIndex := min(rf.commitIndex, len(rf.log)-1)

		for i := rf.lastApplied + 1; i <= commitIndex; i++ {
			DPrintf("[%d] $ Apply index %d, rf.commitIndex is %d, command %v\n, log is %v\n", rf.me, i, commitIndex, rf.log[i], rf.log)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Message, CommandIndex: i}
			rf.applyCh <- msg
			rf.lastApplied = i
			DPrintf("[%d] $ lastApplied set to %d\n", rf.me, i)
		}
		rf.mu.Unlock()
		// time.Sleep(apply_detect_interval)
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	// Your code here (2B).
	// DPrintf("[%d] * Handle get lock %v\n", rf.me, command)
	rf.log = append(rf.log, entry{Term: rf.currentTerm, Message: command})
	index := rf.getLastIndex()
	DPrintf("[%d] *Client's message %v, index=%d, term=%d\n", rf.me, command, index, term)
	rf.persist()
	// DPrintf("[%d] ** start  persist\n", rf.me)

	go rf.handle(command)

	return index, term, isLeader

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[%d] /// rf.state=%d, rf.currentTerm=%d, len(rf.log)=%d, commitIndex=%d, matchIndex=%v, lastApplied=%d, log is %v\n",
		rf.me, rf.state, rf.currentTerm, len(rf.log), rf.commitIndex, rf.matchIndex, rf.lastApplied, rf.log)

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	time.Sleep(100 * time.Millisecond)
	rf := &Raft{peers: peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		currentTerm:     0,
		commitIndex:     0,
		lastApplied:     0,
		electionTimer:   time.NewTicker(RandomizedElectionTimeout()),
		heartbeatTimer:  time.NewTicker(heartbeat_timeout),
		leader_cond:     sync.NewCond(&sync.Mutex{}),
		replicator_cond: make([]*sync.Cond, len(peers)),
	}
	rf.mu.Lock()
	rf.log = append(rf.log, entry{Term: 0})
	rf.ChangeState(STATE_FOLLOWER)

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	go rf.alive()
	go rf.leader_hb()

	for i := range rf.peers {
		if i != rf.me {
			rf.replicator_cond[i] = sync.NewCond(&sync.Mutex{})

			go rf.replicator(i)
		}
	}
	// go rf.election()
	go rf.applier()

	return rf
}
