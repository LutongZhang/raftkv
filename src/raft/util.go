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
	r := rf.role
	rf.mu.Unlock()
	return r
}

func (rf *Raft) changeToFollower(newTerm int64, votedFor int) {
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
		go rf.persist()
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
		go rf.persist()
	}
}

func (rf *Raft) changeToLeader() {
	rf.role = Leader
	//rf.timeStart = time.Now()
	rf.setNewElectionPoint()
}

//
func (rf *Raft) appendLog(entry *LogEntry) {
	rf.log = append(rf.log, *entry)
	go rf.persist()
}

func (rf *Raft) appendLogs(entrys []LogEntry) {
	rf.log = append(rf.log, entrys...)
	go rf.persist()
}

func (rf *Raft) getLogSliceIdx(i int) int {
	return len(rf.log) - (int(rf.log[len(rf.log)-1].Idx) - i + 1)
}

//
func (rf *Raft) setNewElectionPoint() {
	rf.electioTimePoint = time.Now().Add(randomElectionTimeOut())
}
func randomElectionTimeOut() time.Duration {
	//old 150+150
	return time.Duration(rand.Int31n(150)+150) * time.Millisecond
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
