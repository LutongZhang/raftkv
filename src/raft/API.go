package raft

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = int(rf.currentTerm)
	var isleader = rf.role == Leader
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapShotSize() int {
	return rf.persister.SnapshotSize()
}

func (rf *Raft)ReadSnapshot()[]byte{
	return rf.persister.ReadSnapshot()
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
	if rf.role != Leader {
		return -1, -1, false
	}
	prevLog := rf.log[len(rf.log)-1]
	newIdx := prevLog.Idx + 1
	term := rf.currentTerm
	rf.appendLog(&LogEntry{
		Command: command,
		Term:    term,
		Idx:     newIdx,
	})
	rf.log_infof("append %v with with commitIdx %d lastApplied %d",rf.log[len(rf.log)-1],rf.commitIdx, rf.lastApplied)
	rf.replicateLogs()
	return int(newIdx), int(term), true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastSnapShotLog := rf.log[0]
	rf.log_infof("snapshot start with idx %d",index)
	if index > int(lastSnapShotLog.Idx) {
		i := getLogSliceIdx(rf.log, index)
		lastIncludedIdx := rf.log[i].Idx
		lastIncludedTerm := rf.log[i].Term
		rf.log = rf.log[i:] //first one is always previous log
		rf.persistStateAndSnapshot(snapshot)
		//send SnapShot
		if rf.role == Leader {
			rf.installSnapshotToPeers(lastIncludedIdx, lastIncludedTerm, snapshot)
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex >= int(rf.lastApplied) {
		rf.log_infof("CondInstall start with last term %d last idx %d",lastIncludedTerm,lastIncludedIndex )
		rf.lastApplied = int64(lastIncludedIndex) //Todo ????
		if lastIncludedIndex <= int(rf.log[len(rf.log)-1].Idx) {
			i := getLogSliceIdx(rf.log, lastIncludedIndex)
			rf.log = rf.log[i:] //first one is always previous log
		} else {
			rf.log = []LogEntry{
				{
					nil,
					int64(lastIncludedTerm),
					int64(lastIncludedIndex),
				},
			}
		}

		rf.persistStateAndSnapshot(snapshot)

		for i := range rf.peers {
			if rf.nextIdx[i] <= int64(lastIncludedIndex) {
				rf.nextIdx[i] = int64(lastIncludedIndex) + 1
			}
			if rf.matchIdx[i] < int64(lastIncludedIndex) {
				rf.matchIdx[i] = int64(lastIncludedIndex)
			}
		}
		return true
	} else {
		return false
	}
}
