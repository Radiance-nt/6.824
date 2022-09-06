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
	"fmt"
	"labs/src/labrpc"
	"math/rand"
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

	// persistent state
	state       int
	currentTerm int
	votedFor    int
	log         []entry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	election_timeout time.Duration
	last_hear_time   time.Time

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
func (rf *Raft) ChangeState(state int) {
	if state == STATE_FOLLOWER {
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	} else if state == STATE_CANDIDATE {
		rf.state = STATE_CANDIDATE

	} else if state == STATE_LEADER {
		rf.state = STATE_LEADER

	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate_id int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type RequestAppendEntriesArgs struct {
	// Your data here (2A, 2B)..
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int //leader's commitIndex
}

type RequestAppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)

	if args.Candidate_id == rf.me {
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.Term < rf.currentTerm\n", rf.me, args.Candidate_id, args.Term)

		return
	}
	// fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, aquire the lock\n", rf.me, args.Candidate_id, args.Term)

	// *********  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log (See 5.4.1), grant vote
	rf.mu.Lock()
	fmt.Printf("[%d] --- vote lock \n", rf.me)

	// fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, get the lock\n", rf.me, args.Candidate_id, args.Term)

	defer rf.mu.Unlock()
	defer fmt.Printf("[%d] --- vote lock \n", rf.me)

	if args.Term > rf.currentTerm {
		fmt.Printf("[%d] --- Turn to follower, recieved from machine%d, args.Term %d > rf.currentTerm %d\n", rf.me, args.Candidate_id, args.Term, rf.currentTerm)
		rf.ChangeState(STATE_FOLLOWER)
	}
	if !(rf.state == STATE_FOLLOWER && rf.votedFor == -1) {
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because already voted\n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	}
	// if time.Since(rf.last_hear_time) <= time.Duration(election_timout_minimun) {
	// 	reply.VoteGranted = false
	// 	return
	// }

	fmt.Printf("[%d] --- Vote for %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)

	// more up to date last term
	if args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex()) {
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex() \n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.Candidate_id
	reply.VoteGranted = true
	rf.ChangeState(STATE_CANDIDATE)

	fmt.Printf("[%d] --- Vote for %d whose term is %d done\n", rf.me, args.Candidate_id, args.Term)

}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	fmt.Printf("[%d] ***AE get lock\n", rf.me)
	defer rf.mu.Unlock()
	defer fmt.Printf("[%d] ***AE unlock\n", rf.me)

	if args.Term < rf.currentTerm {
		if len(args.Entries) == 0 {
			fmt.Printf("[%d] Recieved HB: args.Term %d < rf.currentTerm %d from %d\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)

		} else {
			fmt.Printf("[%d] *** Recieved Appending, args.Term %d < rf.currentTerm %d from %d\n", rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE) {
		if rf.state == STATE_LEADER {
			fmt.Printf("[%d] Heard HB, From Leader to follower, args.Term %d < rf.currentTerm %d from %d\n",
				rf.me, args.Term, rf.currentTerm, args.LeaderId)
		} else if args.Term == rf.currentTerm && rf.state == STATE_CANDIDATE {
			fmt.Printf("[%d] Heard HB, From Candidate to follower, args.Term %d < rf.currentTerm %d from %d\n",
				rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		rf.currentTerm = args.Term
		rf.ChangeState(STATE_FOLLOWER)
	}

	if len(args.Entries) == 0 {
		fmt.Printf("[%d] Heard Hearbeat from %d of %d term \n", rf.me, args.LeaderId, args.Term)
		reply.Success = true
		rf.update_commit(*args)
		rf.last_hear_time = time.Now()

		return
	}
	// return false if log doesn't contain an entry matching prevLogIndex whose term matches preLogTerm, follow rule no.2
	fmt.Printf("[%d] ***  Heard appending from %d of term %d, len(rf.log) %d, PrevLogIndex=%d \n",
		rf.me, args.LeaderId, args.Term, len(rf.log), args.PrevLogIndex)

	if args.PrevLogIndex >= 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		if args.PrevLogIndex < 0 {
			fmt.Printf("[%d] ***  Respond failed because args.PrevLogIndex %d< 0 \n", rf.me, args.PrevLogIndex)
		} else {
			fmt.Printf("[%d] ***  Respond failed, len(rf.log) %d, args.PrevLogIndex %d \n", rf.me, len(rf.log), args.PrevLogIndex)
		}
		reply.Success = false
		return
	}
	fmt.Printf("[%d] ***  Respond appending success to %d, PrevLogIndex=%d \n", rf.me, args.LeaderId, args.PrevLogIndex)

	reply.Success = true
	rf.last_hear_time = time.Now()

	next_log := args.PrevLogIndex + 1
	// delete its own log and follow the leader
	if len(rf.log) > next_log && rf.log[next_log].Term != args.Term {
		fmt.Printf("[%d] *** Delete rf.log = rf.log[:next_log %d] , and len(rf.log) is %d \n", rf.me, next_log, len(rf.log))
		rf.log = rf.log[:next_log]
		fmt.Printf("[%d] *** and Now len(rf.log) %d \n", rf.me, len(rf.log))
	}
	fmt.Printf("[%d] **** Leader%d commit=%d, rf.commitIndex=%d, lastApplied=%d\n",
		rf.me, args.LeaderId, args.LeaderCommit, rf.commitIndex, rf.lastApplied)
	rf.log = append(rf.log, args.Entries...)
	fmt.Printf("[%d] **** Appending log %v done, and len(rf.log) is %d, log is %v \n", rf.me, args.Entries, len(rf.log), rf.log)

	rf.update_commit(*args)

}
func (rf *Raft) update_commit(args RequestAppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		fmt.Printf("[%d] **** rf.commitIndex = min(args.LeaderCommit %d, rf.getLastIndex() %d)\n",
			rf.me, args.LeaderCommit, rf.getLastIndex())

	}
	for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
		fmt.Printf("[%d] $$$ Apply index %d, rf.commitIndex is %d, command %v\n", rf.me, i, rf.commitIndex, rf.log[i])
		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Message, CommandIndex: i}
		rf.applyCh <- msg
		rf.lastApplied = i
		fmt.Printf("[%d] $$$ lastApplied set to %d\n", rf.me, i)

	}

	// if rf.commitIndex > rf.lastApplied {
	// 	fmt.Printf("[%d] $$$$$  rf.commitIndex %d> rf.lastApplie %d\n", rf.me, rf.commitIndex, rf.lastApplied)

	// 	rf.lastApplied = rf.commitIndex
	// 	// NEXT SATGE APPLY STATE MACHINE
	// }

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sender(i int) {
	term := rf.currentTerm
	for {
		if rf.currentTerm <= term && rf.getLastIndex() >= rf.nextIndex[i] {
			// sendHB(i, false)
		} else {
		}
	}
}

func (rf *Raft) sendHB(i int, heartbeat bool) {
	var args RequestAppendEntriesArgs
	var prevLogIndex int
	var prevLogTerm int

	rf.mu.Lock()
	fmt.Printf("[%d] #* send get lock\n", rf.me)
	prevLogIndex = rf.nextIndex[i] - 1
	prevLogTerm = rf.getIndexTerm(prevLogIndex)

	fmt.Printf("[%d] #* Send hb to %d, prevLogIndex= %d, len(rf.log) %d, Entries %v\n",
		rf.me, i, prevLogIndex, len(rf.log), rf.log[prevLogIndex+1:])

	if !heartbeat {
		args = RequestAppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      rf.log[prevLogIndex+1:],
			PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
	} else {
		args = RequestAppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,

			PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
	}
	reply := RequestAppendEntriesReply{}

	if rf.getLastIndex() >= rf.nextIndex[i] {
		fmt.Printf("[%d] ** Update %d log, prevLogIndex= %d \n", rf.me, i, prevLogIndex)
	} else {
		fmt.Printf("[%d] ## Machine %d up to date, send normal hb, len(entries)=%d\n",
			rf.me, i, len(rf.log[prevLogIndex+1:]))
	}
	rf.mu.Unlock()
	fmt.Printf("[%d] #* send HB unlock\n", rf.me)
	ok := rf.sendHeartBeat(i, &args, &reply)
	if !ok {
		fmt.Printf("[%d] #* sendHeartBeat not ok from %d \n", rf.me, i)
		return
	}
	// Does it need to lock on and check if rf.currentTerm > term?
	rf.mu.Lock()
	fmt.Printf("[%d] #* send HB reply get lock\n", rf.me)
	if reply.Success {
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		rf.nextIndex[i] += len(args.Entries)
		if len(args.Entries) > 0 {
			fmt.Printf("[%d] ** Machine%d reply success, matchIndex=%d, nextIndex=%d, len(args.Entries)=%d \n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], len(args.Entries))
		}

	} else if reply.Term > rf.currentTerm {
		fmt.Printf("[%d] ** Turn to follower, recieved from machine%d, reply.Term %d > rf.currentTerm %d\n",
			rf.me, i, reply.Term, rf.currentTerm)
		rf.currentTerm = args.Term

		rf.ChangeState(STATE_FOLLOWER)
		rf.mu.Unlock()
		fmt.Printf("[%d] #* send HB reply unlock\n", rf.me)

		return
	} else {
		fmt.Printf("[%d] ** Failed to update %d log, rf.nextIndex -1 from %d \n", rf.me, i, rf.nextIndex[i])
		if rf.nextIndex[i] < 0 {
			fmt.Printf("[%d] ** FATAL: rf.nextIndex[i] < 0, args:%v, reply: %v \n",
				rf.me, args, reply)
		}

		rf.nextIndex[i] -= 1
		rf.mu.Unlock()
		fmt.Printf("[%d] #* send HB reply unlock\n", rf.me)
		return
	}
	rf.mu.Unlock()
	fmt.Printf("[%d] #* send HB reply unlock\n", rf.me)

}

func (rf *Raft) leader() {
	// TODO: May have a huge bug
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	fmt.Printf("[%d] leader() set rf.nextIndex %d \n", rf.me, rf.commitIndex+1)

	for index := range rf.nextIndex {
		rf.nextIndex[index] = rf.commitIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for index := range rf.matchIndex {
		rf.matchIndex[index] = -1
	}
	rf.mu.Unlock()

	term := rf.currentTerm
	// TODO: DEFER Finishing Ledaer
	for !rf.killed() {
		fmt.Printf("[%d] # leader() starting to sendHeartBeat of %d term to everyone \n", rf.me, rf.currentTerm)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendHB(i, true)
		}
		time.Sleep(heartbeat_timeout)
		if rf.state != STATE_LEADER {
			fmt.Printf("[%d] # Back to follower after sending Hearbeat\n", rf.me)
			return
		}
		if rf.currentTerm > term {
			fmt.Printf("[%d] # Learder changed... stop sending HB\n", rf.me)
			return
		}
	}
}
func (rf *Raft) request(args RequestVoteArgs, i int, count *int, count_lock *sync.Mutex) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply)
	if ok {
		if reply.VoteGranted {
			fmt.Printf("[%d] -- Recieve poll from %d \n", rf.me, i)
			(*count_lock).Lock()
			*count += 1
			(*count_lock).Unlock()
			return

		}
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			fmt.Printf("[%d] --- request lock \n", rf.me)
			fmt.Printf("[%d] -- Recieved reply from machine%d, reply.Term %d > rf.currentTerm %d\n", rf.me, i, args.Term, rf.currentTerm)
			rf.currentTerm = reply.Term

			rf.ChangeState(STATE_FOLLOWER)

			rf.mu.Unlock()
			fmt.Printf("[%d] --- request unlock \n", rf.me)

			return
		}
	} else {
		fmt.Printf("[%d] -- sendRequestVote not ok from %d \n", rf.me, i)
	}
}

// var notOK_wait time.Duration = time.Millisecond * time.Duration(1000)
var heartbeat_timeout time.Duration = time.Millisecond * time.Duration(100)
var waitVote_timeout time.Duration = 5 * time.Millisecond
var election_timout_minimun int = 200
var election_timout_rand int = 200

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getIndexTerm(index int) int {
	if index < 0 {
		return 0
	}
	return rf.log[index].Term
}

func (rf *Raft) raiseElection() {
	if rf.votedFor == rf.me {

		fmt.Printf("[%d] - Election time out, starting an election %d\n", rf.me, rf.currentTerm)

		term := rf.currentTerm
		args := RequestVoteArgs{Term: term, Candidate_id: rf.me, LastLogIndex: rf.getLastIndex(), LastLogTerm: rf.getIndexTerm(rf.getLastIndex())}
		count := 0
		var count_lock sync.Mutex
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.request(args, i, &count, &count_lock)
		}

		time.Sleep(waitVote_timeout)
		fmt.Printf("[%d] - Poll count: %d\n", rf.me, count)

		rf.mu.Lock()
		fmt.Printf("[%d] --- raise election lock \n", rf.me)

		if rf.state != STATE_CANDIDATE {
			rf.mu.Unlock()
			fmt.Printf("[%d] --- raise election unlock \n", rf.me)

			return
		}
		if count+1 > len(rf.peers)/2 {
			if rf.state == STATE_FOLLOWER {
				fmt.Printf("[%d] - FATAL: FALL TO FOLLOWER BUT RECIEVE OVER HALF POLLS!\n", rf.me)
			}
			rf.ChangeState(STATE_LEADER)

			fmt.Printf("[%d] - Being a leader at term %d\n", rf.me, rf.currentTerm)

			rf.mu.Unlock()
			fmt.Printf("[%d] --- raise election unlock \n", rf.me)

			return
		}
		rf.mu.Unlock()
		fmt.Printf("[%d] --- raise election unlock \n", rf.me)

	}
	rf.last_hear_time = time.Now()
	rf.follower()

}

func (rf *Raft) follower() {
	rf.election_timeout = time.Duration(rand.Intn(election_timout_rand)+election_timout_minimun) * time.Millisecond
	time.Sleep(rf.election_timeout)
	for !rf.killed() {
		if time.Since(rf.last_hear_time) > rf.election_timeout {
			rf.mu.Lock()
			rf.ChangeState(STATE_CANDIDATE)
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.mu.Unlock()
			return
		} else {
			// rf.election_timeout = time.Duration(rand.Intn(300)+election_timout_minimun) * time.Millisecond
			// if rf.election_timeout-time.Since(rf.last_hear_time) > 0 {
			// 	time.Sleep(rf.election_timeout - time.Since(rf.last_hear_time))
			// }
			return

		}

	}
}

func (rf *Raft) alive() {
	for !rf.killed() {
		if rf.state == STATE_FOLLOWER {
			rf.follower()
		} else if rf.state == STATE_CANDIDATE {
			rf.raiseElection()

		} else {
			rf.leader()
		}

	}
}

func (rf *Raft) handle(command interface{}) {
	fmt.Printf("[%d] * Tried to Handle client's message %v, log is %v\n", rf.me, command, rf.log)

	rf.mu.Lock()
	fmt.Printf("[%d] * Handle get lock %v\n", rf.me, command)

	term, _ := rf.GetState()
	rf.log = append(rf.log, entry{Term: rf.currentTerm, Message: command})
	commitIndex := rf.getLastIndex()
	fmt.Printf("[%d] * Handle client's message %v, log is %v, my last is %d\n", rf.me, command, rf.log, commitIndex)
	rf.mu.Unlock()

	// count := 0
	// var count_lock sync.Mutex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

	}
	// t := time.Now()
	time.Sleep(waitVote_timeout)
	// fmt.Printf("[%d] * Appending count: %d\n", rf.me, count)
	// fmt.Printf("[%d] interval: %v\n", rf.me, time.Since(t))

	if rf.currentTerm > term {
		return
	}
	rf.mu.Lock()
	n := rf.commitIndex
	for !rf.killed() {
		c := 0
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] > n {
				c += 1
			}
		}
		if !(n < 0 || (n < len(rf.log) && rf.log[n].Term == rf.currentTerm && (c+1 >= len(rf.peers)/2))) {
			fmt.Printf("[%d] * rf.commitIndex set to %d\n", rf.me, n-1)
			break
		}
		n += 1
	}
	rf.commitIndex = n - 1
	rf.mu.Unlock()

	for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
		fmt.Printf("[%d] $ Apply index %d, rf.commitIndex is %d, command %v\n", rf.me, i, rf.commitIndex, rf.log[i])
		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Message, CommandIndex: i}
		rf.applyCh <- msg
		rf.lastApplied = i
		fmt.Printf("[%d] $ lastApplied set to %d\n", rf.me, i)

	}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLastIndex() + 1
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	// Your code here (2B).
	go rf.handle(command)
	defer fmt.Printf("[%d] *Client's message %v, index=%d, term=%d\n", rf.me, command, index, term)

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
	fmt.Printf("[%d] /// rf.state=%d, rf.currentTerm=%d, len(rf.log)=%d, commitIndex=%d, lastApplied=%d, log is %v\n",
		rf.me, rf.state, rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied, rf.log)

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
	rf := &Raft{peers: peers,
		persister:      persister,
		me:             me,
		applyCh:        applyCh,
		last_hear_time: time.Now(),
		currentTerm:    0,
		commitIndex:    -1,
		lastApplied:    -1}

	rf.ChangeState(STATE_FOLLOWER)

	// Your initialization code here (2A, 2B, 2C).
	go rf.alive()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
