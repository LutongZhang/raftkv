package raft

import "fmt"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term = int(rf.currentTerm)
	var isleader = rf.role == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
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
	fmt.Println(fmt.Sprintf("%s %d append %v in log %v with commitIdx %d lastApplied %d", roleStr(rf.role), rf.me, rf.log[len(rf.log)-1], rf.log, rf.commitIdx, rf.lastApplied))
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
	fmt.Println("Snapshot start", roleStr(rf.role), rf.me, "index", index)
	if index > int(lastSnapShotLog.Idx) {
		rf.persistStateAndSnapshot(snapshot)
		//rf.mu.Lock()
		//defer rf.mu.Unlock()
		i := getLogSliceIdx(rf.log, index)
		lastIncludedIdx := rf.log[i].Idx
		lastIncludedTerm := rf.log[i].Term
		rf.log = rf.log[i:] //first one is always previous log
		//send SnapShot
		//fmt.Println("yyyy SnapShot", roleStr(rf.role), rf.me, "lastIDx", lastIncludedIdx)
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
	fmt.Println("CondInstall start", roleStr(rf.role), rf.me, "lastIDx", lastIncludedIndex)
	//lastSnapShotLog := rf.log[0]
	//if lastIncludedIndex >= int(lastSnapShotLog.Idx) { //Todo 是不是应该比较last Snapshot Idx？
	if lastIncludedIndex >= int(rf.lastApplied) {
		rf.persistStateAndSnapshot(snapshot)
		//rf.mu.Lock()
		//defer rf.mu.Unlock()
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
		rf.lastApplied = int64(lastIncludedIndex) //Todo ????
		return true
	} else {
		return false
	}
}
