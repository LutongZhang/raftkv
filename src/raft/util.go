package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) getRoleL() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := rf.role
	return r
}

func (rf *Raft) changeToFollower(newTerm int64, votedFor int) {
	if rf.role == Leader {
		rf.log_info("change to follower from leader")
	}
	rf.role = Follower
	rf.setNewElectionPoint()
	needPersist := false
	if rf.currentTerm != newTerm {
		rf.currentTerm = newTerm
		needPersist = true
	}
	if votedFor != rf.votedFor {
		rf.votedFor = votedFor
		needPersist = true
	}
	if needPersist {
		go rf.persistStateL()
	}
}

func (rf *Raft) changeToCandidate(newTerm int64, votedFor int) {
	rf.setNewElectionPoint()
	rf.role = Candidate
	needPersist := false
	if rf.currentTerm != newTerm {
		rf.currentTerm = newTerm
		needPersist = true
	}
	if votedFor != rf.votedFor {
		rf.votedFor = votedFor
		needPersist = true
	}
	if needPersist {
		go rf.persistStateL()
	}
}

func (rf *Raft) changeToLeader() {
	rf.role = Leader
	rf.log_info("become leader")
	//rf.timeStart = time.Now()
	for i, _ := range rf.nextIdx {
		rf.nextIdx[i] = rf.log[len(rf.log)-1].Idx + 1
	}
	rf.setNewElectionPoint()
}

//
func (rf *Raft) appendLog(entry *LogEntry) {
	rf.log = append(rf.log, *entry)
	go rf.persistStateL()
}

func (rf *Raft) appendLogs(entrys []LogEntry) {
	rf.log = append(rf.log, entrys...)
	go rf.persistStateL()
}

//
func (rf *Raft) setNewElectionPoint() {
	rf.electioTimePoint = time.Now().Add(randomElectionTimeOut())
}

func randomElectionTimeOut() time.Duration {
	//old 150+150
	//return time.Duration(rand.Int31n(150)+150) * time.Millisecond
	return time.Duration(rand.Int31n(500)+300) * time.Millisecond
}

func getLogSliceIdx(log []LogEntry, i int) int {
	return len(log) - (int(log[len(log)-1].Idx) - i + 1)
}

//
func roleStr(i Role) string {
	if i == Follower {
		return "Follower"
	} else if i == Leader {
		return "Leader"
	} else {
		return "Candidate"
	}
}

func max(a int64, b int64) int64 {
	if a >= b {
		return a
	} else {
		return b
	}
}

func min(a int64, b int64) int64 {
	if a <= b {
		return a
	} else {
		return b
	}
}
