package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	heartbeat_timeout = time.Millisecond * time.Duration(50)

	waitVote_timeout = 5 * time.Millisecond
	// waitVote_timeout        = 5 * time.Millisecond
	election_timout_minimun = 150
	election_timout_rand    = 50
)

func (rf *Raft) ResetelectionTimer() {
	rf.electionTimer.Reset(RandomizedElectionTimeout())
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(election_timout_rand)+election_timout_minimun) * time.Millisecond
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidate_id int

	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

func (rf *Raft) ChangeState(state int) {
	if state == STATE_FOLLOWER {
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		// rf.ResetelectionTimer()
	} else if state == STATE_CANDIDATE {
		rf.state = STATE_CANDIDATE

	} else if state == STATE_LEADER {

		rf.nextIndex = make([]int, len(rf.peers))
		DPrintf("[%d] leader() set rf.nextIndex %d \n", rf.me, rf.commitIndex+1)

		for index := range rf.nextIndex {
			rf.nextIndex[index] = rf.commitIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for index := range rf.matchIndex {
			rf.matchIndex[index] = 0
		}

		rf.state = STATE_LEADER
		/////////////// No-op <To pass Lab2 you need to annotate the following part> ///////////////
		rf.log = append(rf.log, entry{Term: rf.currentTerm, Message: nil})
		rf.persist()
		DPrintf("[%d] Being a Leader NO-OP %d \n", rf.me, rf.commitIndex+1)
		for i := range rf.peers {
			if i != rf.me {
				rf.replicator_cond[i].Signal()
			}
		}
		time.Sleep(heartbeat_timeout)
		rf.commitIndex = rf.findLargestcommitIndex()
		///////////////////////////////////////////////////////////////////////////////////////////
		rf.leader_cond.Signal()

	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("[%d] --- Recieved requset vote from %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// DPrintf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.Term < rf.currentTerm\n", rf.me, args.Candidate_id, args.Term)
		rf.mu.Unlock()
		return
	}

	defer rf.mu.Unlock()
	defer rf.persist()
	// defer DPrintf("[%d]  defer req votes rpc  persist\n", rf.me)

	if args.Term > rf.currentTerm {
		// DPrintf("[%d] --- Turn to follower, recieved from machine%d, args.Term %d > rf.currentTerm %d\n", rf.me, args.Candidate_id, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.ChangeState(STATE_FOLLOWER)
	}
	if rf.votedFor != -1 {
		// DPrintf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because already voted\n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	}
	// more up to date last term
	// args.LastLogIndex < rf.getLastIndex() ||
	if args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex()) {
		// DPrintf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex() \n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	} else if args.LastLogTerm == rf.getIndexTerm(rf.getLastIndex()) && args.LastLogIndex < rf.getLastIndex() {
		// DPrintf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.LastLogTerm == rf.getIndexTerm(rf.getLastIndex() but longer \n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	}
	// optimization option
	// if time.Since(rf.last_hear_time) <= time.Duration(election_timout_minimun) {
	// 	reply.VoteGranted = false
	// 	return
	// }
	rf.currentTerm = args.Term
	rf.votedFor = args.Candidate_id
	reply.VoteGranted = true
	// DPrintf("[%d] --- Vote for %d whose term is %d, at this time log is %v\n", rf.me, args.Candidate_id, args.Term, rf.log)
	rf.ResetelectionTimer()
}

func (rf *Raft) request(args RequestVoteArgs, i int, count *int, count_lock *sync.Mutex) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply)
	if !ok {
		DPrintf("[%d] -- sendRequestVote not ok from %d \n", rf.me, i)
		return
	}
	if reply.VoteGranted {
		DPrintf("[%d] -- Recieve poll from %d \n", rf.me, i)
		(*count_lock).Lock()
		*count += 1
		(*count_lock).Unlock()
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {

		// DPrintf("[%d] --- request lock \n", rf.me)
		DPrintf("[%d] -- Recieved reply from machine%d, reply.Term %d > rf.currentTerm %d\n", rf.me, i, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.ChangeState(STATE_FOLLOWER)

		// DPrintf("[%d] request() reply term larger  persist\n", rf.me)
		rf.persist()
		// DPrintf("[%d] --- request unlock \n", rf.me)
		return
	}

}
func (rf *Raft) raiseElection() {
	raise_start := time.Now()
	rf.mu.Lock()
	rf.ChangeState(STATE_CANDIDATE)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.ResetelectionTimer()
	DPrintf("[%d] - Election time out, starting an election %d\n", rf.me, rf.currentTerm)
	// DPrintf("[%d]  - Election persist\n", rf.me)

	rf.persist()

	term := rf.currentTerm
	args := RequestVoteArgs{Term: term, Candidate_id: rf.me, LastLogIndex: rf.getLastIndex(), LastLogTerm: rf.getIndexTerm(rf.getLastIndex())}
	count := 0
	rf.mu.Unlock()
	var count_lock sync.Mutex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.request(args, i, &count, &count_lock)
	}

	for time.Since(raise_start) < election_timout_minimun*time.Millisecond {
		rf.mu.Lock()
		// DPrintf("[%d] --- raise election lock \n", rf.me)

		if rf.state != STATE_CANDIDATE {
			// DPrintf("[%d] --- raise election unlock \n", rf.me)
			DPrintf("[%d] --- Return because state is %d \n", rf.me, rf.state)
			rf.mu.Unlock()

			return
		}
		count_lock.Lock()
		if count+1 > len(rf.peers)/2 {
			// if rf.state == STATE_FOLLOWER {
			// 	DPrintf("[%d] - FATAL: FALL TO FOLLOWER BUT RECIEVE OVER HALF POLLS!\n", rf.me)
			// }
			DPrintf("[%d] - Poll count: %d\n", rf.me, count)

			DPrintf("[%d] - Being a leader at term, log is %v %d\n", rf.me, rf.currentTerm, rf.log)

			rf.ChangeState(STATE_LEADER)
			DPrintf("[%d]  vote enough  persist\n", rf.me)
			count_lock.Unlock()

			rf.persist()
			rf.mu.Unlock()
			return
		}
		count_lock.Unlock()

		rf.mu.Unlock()
		time.Sleep(waitVote_timeout)
	}

	DPrintf("[%d] --- Didn't get enough polls, count+1 %d> len(rf.peers)/2 %d,  state is %d \n", rf.me, count+1, len(rf.peers)/2, rf.state)

}
func (rf *Raft) alive() {
	rf.ResetelectionTimer()
	defer rf.electionTimer.Stop()

	for !rf.killed() {
		<-rf.electionTimer.C
		if rf.state != STATE_LEADER {
			rf.raiseElection()
			DPrintf("[%d] - Finish election \n", rf.me)

		}
	}
}
func (rf *Raft) leader_hb() {
	var term int
	for !rf.killed() {
		rf.leader_cond.L.Lock()
		for !rf.killed() && rf.state != STATE_LEADER {
			// time.Sleep(10 * time.Millisecond)
			rf.leader_cond.Wait()
		}
		DPrintf("[%d] # Being a leader \n", rf.me)
		rf.leader_cond.L.Unlock()

		rf.heartbeatTimer.Reset(heartbeat_timeout)
		term = rf.currentTerm
		for !rf.killed() && rf.currentTerm == term && rf.state == STATE_LEADER {
			DPrintf("[%d] # leader() starting to sendHeartBeat of %d term to everyone \n", rf.me, rf.currentTerm)

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendHB(i)
			}
			rf.mu.Lock()
			rf.commitIndex = rf.findLargestcommitIndex()
			rf.mu.Unlock()
			<-rf.heartbeatTimer.C
		}
		rf.heartbeatTimer.Stop()
		DPrintf("[%d] # Back to follower because Learder changed... \n", rf.me)
	}
}
