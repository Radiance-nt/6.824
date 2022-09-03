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

type logType interface{}

type entry struct {
	Term    int
	Message logType
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
	fmt.Printf("[%d] Recieved requset vote from %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)

	if args.Candidate_id == rf.me {
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// *********  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log (See 5.4.1), grant vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !(rf.state == STATE_FOLLOWER && rf.votedFor == -1) {
		reply.VoteGranted = false
		return
	}
	if time.Since(rf.last_hear_time) <= time.Duration(election_timout_minimun) {
		reply.VoteGranted = false
		return
	}
	// more up to date last term
	if args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex()) {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.Candidate_id
	reply.VoteGranted = true

	fmt.Printf("[%d]  Vote for %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)

}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here (2A, 2B).
	// may cause TROUBLE in 2B
	if args.LeaderId == rf.me {
		return
	}

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
	if args.Term > rf.currentTerm {
		if rf.state == STATE_LEADER {
			rf.state = STATE_FOLLOWER
			fmt.Printf("[%d] Heard HB, From Leader to follower, args.Term %d < rf.currentTerm %d from %d\n",
				rf.me, args.Term, rf.currentTerm, args.LeaderId)
		}
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.mu.Unlock()

	}
	if rf.state == STATE_CANDIDATE && args.Term >= rf.currentTerm {
		fmt.Printf("[%d] Heard HB, From Candidate to follower, args.Term %d < rf.currentTerm %d from %d\n",
			rf.me, args.Term, rf.currentTerm, args.LeaderId)
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER
		rf.mu.Unlock()
	}
	if len(args.Entries) == 0 {
		// fmt.Printf("[%d] Heard Hearbeat \n", rf.me)
		reply.Success = true
		return
	}
	// return false if log doesn't contain an entry matching prevLogIndex whose term matches preLogTerm, follow rule no.2
	fmt.Printf("[%d] ***  Heard appending from %d, len(rf.log) %d, PrevLogIndex=%d \n", rf.me, args.LeaderId, len(rf.log), args.PrevLogIndex)

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
	if len(rf.log) > next_log && rf.log[next_log].Term != args.PrevLogTerm {
		fmt.Printf("[%d] *** Delete rf.log = rf.log[:next_log] %d, and len(rf.log) is %d \n", rf.me, next_log, len(rf.log))
		rf.log = rf.log[:next_log]
		fmt.Printf("[%d] *** and Now len(rf.log) %d \n", rf.me, len(rf.log))

	}
	rf.log = append(rf.log, args.Entries...)
	fmt.Printf("[%d] *** Appending log %v done, and len(rf.log) is %d, log is %v \n", rf.me, args.Entries, len(rf.log), rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
		// NEXT SATGE APPLY STATE MACHINE
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leader() {
	// TODO: Initialize Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for index := range rf.nextIndex {
		rf.nextIndex[index] = rf.commitIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for index := range rf.matchIndex {
		rf.matchIndex[index] = -1
	}
	// TODO: DEFER Finishing Ledaer
	for {
		args := RequestAppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			reply := RequestAppendEntriesReply{}
			ok := rf.sendHeartBeat(i, &args, &reply)
			if !ok {
				fmt.Printf("[%d] sendHeartBeat not ok from %d \n", rf.me, i)
			} else {
				if i != rf.me && !reply.Success {
					fmt.Printf("[%d] Hearbeat Recieve from %d, status: %v\n", rf.me, i, reply.Success)
				}
			}
		}
		time.Sleep(heartbeat_timeout)
		if rf.state != STATE_LEADER {
			fmt.Printf("[%d] Back to follower after sending Hearbeat\n", rf.me)
			return
		}

	}
}
func (rf *Raft) request(args RequestVoteArgs, i int, count *int, count_lock *sync.Mutex) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply)
	if ok {
		if reply.VoteGranted {
			fmt.Printf("[%d] Recieve poll from %d \n", rf.me, i)
			(*count_lock).Lock()
			*count += 1
			(*count_lock).Unlock()
			return

		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
	} else {
		fmt.Printf("[%d] sendRequestVote not ok from %d \n", rf.me, i)
	}
}

func (rf *Raft) replicate(i int, count *int, count_lock *sync.Mutex) {

	var args RequestAppendEntriesArgs

	var prevLogIndex int
	var prevLogTerm int
	fmt.Printf("[%d] ** Trying to replicate to %d, whose nextIndex== %d. At this moment my last is %d\n", rf.me, i, rf.nextIndex[i], rf.getLastIndex())

	for {
		if rf.getLastIndex() >= rf.nextIndex[i] {
			prevLogIndex = rf.nextIndex[i] - 1
			prevLogTerm = rf.getIndexTerm(prevLogIndex)
			fmt.Printf("[%d] ** Update %d log, prevLogIndex= %d \n", rf.me, i, prevLogIndex)
			args = RequestAppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex,
				Entries:      rf.log[prevLogIndex+1:],
				PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
		} else {
			fmt.Printf("[%d] ** Machine %d up to date\n", rf.me, i)
			(*count_lock).Lock()
			*count += 1
			(*count_lock).Unlock()
			return

		}
		reply := RequestAppendEntriesReply{}
		ok := rf.sendHeartBeat(i, &args, &reply)
		fmt.Printf("[%d] ** Sending to %d, whose nextIndex== %d. Content is %v\n", rf.me, i, rf.nextIndex[i], args)

		if ok {
			if reply.Success {
				// TODO: Match index is what
				rf.matchIndex[i] = rf.nextIndex[i]
				rf.nextIndex[i] += 1
				fmt.Printf("[%d] ** Match %d log %d \n", rf.me, i, rf.matchIndex[i])

			} else if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			} else {
				fmt.Printf("[%d] ** Failed to update %d log, rf.nextIndex -1 from %d \n", rf.me, i, rf.nextIndex[i])
				rf.mu.Lock()
				rf.nextIndex[i] -= 1
				rf.mu.Unlock()

			}
		} else {
			fmt.Printf("[%d] * sendRequestVote not ok from %d \n", rf.me, i)
			time.Sleep(notOK_wait)
		}
	}
}

var notOK_wait time.Duration = time.Millisecond * time.Duration(1000)
var heartbeat_timeout time.Duration = time.Millisecond * time.Duration(20)
var waitVote_timeout time.Duration = 5 * time.Millisecond
var election_timout_minimun int = 150

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

	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm += 1
	fmt.Printf("[%d] Election time out, starting an election %d\n", rf.me, rf.currentTerm)

	rf.mu.Unlock()
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
	// t := time.Now()
	time.Sleep(waitVote_timeout)
	fmt.Printf("[%d] Poll count: %d\n", rf.me, count)
	// fmt.Printf("[%d] interval: %v\n", rf.me, time.Since(t))

	if rf.currentTerm > term {
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}
	if count+1 > len(rf.peers)/2 {
		if rf.state == STATE_FOLLOWER {
			fmt.Printf("[%d] FATAL: FALL TO FOLLOWER BUT RECIEVE OVER HALF POLLS!\n", rf.me)
		}
		rf.state = STATE_LEADER
		fmt.Printf("[%d] Being a leader at term %d\n", rf.me, rf.currentTerm)
	} else {
		fmt.Printf("[%d] Back to follower state.\n", rf.me)
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER
		rf.mu.Unlock()
	}
}

func (rf *Raft) follower() {
	rf.election_timeout = time.Duration(rand.Intn(300)+election_timout_minimun) * time.Millisecond
	time.Sleep(rf.election_timeout)
	for {
		if time.Since(rf.last_hear_time) > rf.election_timeout {
			rf.state = STATE_CANDIDATE
			return
		} else {
			rf.election_timeout = time.Duration(rand.Intn(300)+election_timout_minimun) * time.Millisecond
			if rf.election_timeout-time.Since(rf.last_hear_time) > 0 {
				time.Sleep(rf.election_timeout - time.Since(rf.last_hear_time))
			}
		}

	}
}

func (rf *Raft) alive() {
	for {
		if rf.state == STATE_FOLLOWER {
			rf.follower()
		} else if rf.state == STATE_CANDIDATE {
			rf.raiseElection()
		} else {
			rf.leader()
		}

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

	index := rf.getLastIndex()
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	// Your code here (2B).
	rf.log = append(rf.log, entry{Term: rf.currentTerm, Message: command})
	fmt.Printf("[%d] * Appending client's message, log is %v, my last is %d\n", rf.me, rf.log, rf.getLastIndex())

	count := 0
	var count_lock sync.Mutex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicate(i, &count, &count_lock)
	}
	// t := time.Now()
	time.Sleep(waitVote_timeout)
	fmt.Printf("[%d] * Appending count: %d\n", rf.me, count)
	// fmt.Printf("[%d] interval: %v\n", rf.me, time.Since(t))

	if rf.currentTerm > term {
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.mu.Unlock()
		return index, term, false
	}
	if count+1 > len(rf.peers)/2 {
		if rf.state == STATE_FOLLOWER {
			fmt.Printf("[%d] * Warning: FALL TO FOLLOWER, some leader exsits\n", rf.me)
		}
		fmt.Printf("[%d] * Coping to over a half machine\n", rf.me)
	}
	n := rf.commitIndex
	for {
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
	fmt.Printf("[%d] /// rf.currentTerm=%d, len(rf.log)=%d, log is %v\n", rf.me, rf.currentTerm, len(rf.log), rf.log)

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.last_hear_time = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = STATE_FOLLOWER
	rf.lastApplied = -1
	rf.commitIndex = -1
	// Your initialization code here (2A, 2B, 2C).
	go rf.alive()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
