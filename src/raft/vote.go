package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	heartbeat_timeout = time.Millisecond * time.Duration(50)

	waitVote_timeout        = 20 * time.Millisecond
	election_timout_minimun = 150
	election_timout_rand    = 50
)

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
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	} else if state == STATE_CANDIDATE {
		rf.state = STATE_CANDIDATE
	} else if state == STATE_LEADER {
		rf.state = STATE_LEADER
		rf.leader_cond.Signal()

	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.Term < rf.currentTerm\n", rf.me, args.Candidate_id, args.Term)
		return
	}
	// fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, aquire the lock\n", rf.me, args.Candidate_id, args.Term)

	// *********  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log (See 5.4.1), grant vote
	rf.mu.Lock()
	// fmt.Printf("[%d] --- vote lock \n", rf.me)

	// fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, get the lock\n", rf.me, args.Candidate_id, args.Term)

	defer rf.mu.Unlock()
	// defer fmt.Printf("[%d] --- vote unlock \n", rf.me)

	if args.Term > rf.currentTerm {
		fmt.Printf("[%d] --- Turn to follower, recieved from machine%d, args.Term %d > rf.currentTerm %d\n", rf.me, args.Candidate_id, args.Term, rf.currentTerm)
		rf.ChangeState(STATE_FOLLOWER)
	}
	if rf.votedFor != -1 {
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because already voted\n", rf.me, args.Candidate_id, args.Term)
		reply.VoteGranted = false
		return
	}
	// more up to date last term
	if args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex()) {
		fmt.Printf("[%d] --- Recieved requset vote from %d whose term is %d, not granted because args.LastLogTerm < rf.getIndexTerm(rf.getLastIndex() \n", rf.me, args.Candidate_id, args.Term)
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
	fmt.Printf("[%d] --- Vote for %d whose term is %d\n", rf.me, args.Candidate_id, args.Term)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
}

func (rf *Raft) request(args RequestVoteArgs, i int, count *int, count_lock *sync.Mutex) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply)
	if !ok {
		fmt.Printf("[%d] -- sendRequestVote not ok from %d \n", rf.me, i)
		return
	}
	if reply.VoteGranted {
		fmt.Printf("[%d] -- Recieve poll from %d \n", rf.me, i)
		(*count_lock).Lock()
		*count += 1
		(*count_lock).Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		// fmt.Printf("[%d] --- request lock \n", rf.me)
		fmt.Printf("[%d] -- Recieved reply from machine%d, reply.Term %d > rf.currentTerm %d\n", rf.me, i, args.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.ChangeState(STATE_FOLLOWER)
		rf.mu.Unlock()
		// fmt.Printf("[%d] --- request unlock \n", rf.me)
		return
	}

}
func (rf *Raft) raiseElection() {

	rf.mu.Lock()
	rf.ChangeState(STATE_CANDIDATE)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	fmt.Printf("[%d] - Election time out, starting an election %d\n", rf.me, rf.currentTerm)

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

	time.Sleep(waitVote_timeout)
	fmt.Printf("[%d] - Poll count: %d\n", rf.me, count)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer fmt.Printf("[%d] --- raise election unlock \n", rf.me)

	// fmt.Printf("[%d] --- raise election lock \n", rf.me)

	if rf.state != STATE_CANDIDATE {
		// fmt.Printf("[%d] --- raise election unlock \n", rf.me)
		fmt.Printf("[%d] --- Return because state is %d \n", rf.me, rf.state)
		return
	}
	if count+1 > len(rf.peers)/2 {
		// if rf.state == STATE_FOLLOWER {
		// 	fmt.Printf("[%d] - FATAL: FALL TO FOLLOWER BUT RECIEVE OVER HALF POLLS!\n", rf.me)
		// }
		fmt.Printf("[%d] - Being a leader at term %d\n", rf.me, rf.currentTerm)

		rf.ChangeState(STATE_LEADER)

		return
	}

	fmt.Printf("[%d] --- Didn't get enough polls, count+1 %d> len(rf.peers)/2 %d,  state is %d \n", rf.me, count+1, len(rf.peers)/2, rf.state)

}
func (rf *Raft) alive() {
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	defer rf.electionTimer.Stop()

	for !rf.killed() {
		<-rf.electionTimer.C
		if rf.state != STATE_LEADER {
			rf.raiseElection()
			fmt.Printf("[%d] - Finish election \n", rf.me)

		}
	}
}
func (rf *Raft) leader_hb() {
	var term int
	for !rf.killed() {
		rf.leader_cond.L.Lock()
		for !rf.killed() && rf.state != STATE_LEADER {
			rf.leader_cond.Wait()
		}
		fmt.Printf("[%d] # Being a leader \n", rf.me)

		rf.leader_cond.L.Unlock()

		rf.mu.Lock()
		term = rf.currentTerm
		rf.nextIndex = make([]int, len(rf.peers))
		fmt.Printf("[%d] leader() set rf.nextIndex %d \n", rf.me, rf.commitIndex+1)

		for index := range rf.nextIndex {
			rf.nextIndex[index] = rf.commitIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for index := range rf.matchIndex {
			rf.matchIndex[index] = 0
		}
		rf.mu.Unlock()

		rf.heartbeatTimer.Reset(heartbeat_timeout)
		for !rf.killed() && rf.currentTerm == term && rf.state == STATE_LEADER {
			fmt.Printf("[%d] # leader() starting to sendHeartBeat of %d term to everyone \n", rf.me, rf.currentTerm)

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendHB(i, true)
			}
			<-rf.heartbeatTimer.C
		}
		rf.heartbeatTimer.Stop()
		fmt.Printf("[%d] # Back to follower because Learder changed... \n", rf.me)
	}
}
