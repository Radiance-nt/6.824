package raft

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot
//2. Create new snapshot file if first chunk (offset is 0)
//3. Write data into snapshot file at given offset
//4. Reply and wait for more data chunks if done is false
//5. Save snapshot file, discard any existing or partial snapshot
//with a smaller index
//6. If existing log entry has same index and term as snapshot’s
//last included entry, retain log entries following it and reply
//7. Discard the entire log
//8. Reset state machine using snapshot contents (and load
//snapshot’s cluster configuration)

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// defer DPrintf("[%d] ** defer appendEntrie  persist\n", rf.me)

	if args.Term < rf.currentTerm {
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

	rf.ResetelectionTimer()
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		index := args.LastIncludedIndex
		rf.discard(index, rf.lastIncludedIndex, args.LastIncludedTerm)
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.log[0].Term = args.LastIncludedTerm
		rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	}
}

func (rf *Raft) discard(index int, lastIncludedIndex int, lastIncludedTerm int) {
	//fmt.Printf("[%d] discard  index %d >= lastIncludedIndex %d, len = %d \n", rf.me, index, lastIncludedIndex, len(rf.log))
	if index-lastIncludedIndex >= len(rf.log) {
		rf.log = rf.log[len(rf.log)-1:]
	} else {
		rf.log = rf.log[index-lastIncludedIndex:]
	}
	//fmt.Printf("[%d] discard  len( log) %v\n", rf.me, len(rf.log))

	rf.log[0].Message = nil

}

func (rf *Raft) SaveSnapshotWithState(index int, snapshot []byte) {
	//fmt.Printf("[%d] Tried to a the lock index %d \n", rf.me, index)

	rf.mu.Lock()
	//fmt.Printf("[%d] Tried to a the lock index %d \n", rf.me, index)

	defer rf.mu.Unlock()
	if index < rf.lastIncludedIndex {
		return
	}
	//fmt.Printf("[%d] SaveSnapshotWithState  index %d >= rf.lastIncludedIndex %d\n", rf.me, index, rf.lastIncludedIndex)

	rf.discard(index, rf.lastIncludedIndex, rf.getIndexTerm(index))
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	//fmt.Printf("[%d] SaveSnapshotWithState done\n", rf.me)

}

func (rf *Raft) sendInstallSnapshot(i int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ChangeState(STATE_FOLLOWER)
		//DPrintf("[%d] ** Turn to follower, when sendHB  persist\n", rf.me)
		rf.persist()
		return
	}
	if args.LastIncludedIndex > rf.matchIndex[i] {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[i] {
		rf.nextIndex[i] = args.LastIncludedIndex + 1
	}
}
