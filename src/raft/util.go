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

func (rf *Raft) getTimeOutL() time.Duration {
	rf.mu.Lock()
	t := rf.timeOutPeriod
	rf.mu.Unlock()
	return t
}

func (rf *Raft) changeToFollower() {
	rf.role = Follower
	//rf.timeStart = time.Now()
	rf.timeOutPeriod = hbTimeOut
}

func (rf *Raft) changeToCandidate() {
	rf.role = Candidate
	//rf.timeStart = time.Now()
	rf.timeOutPeriod = calculateElectionTimeOut()
}

func (rf *Raft) changeToCandidateL() {
	rf.mu.Lock()
	rf.changeToCandidate()
	rf.mu.Unlock()
}

func (rf *Raft) changeToLeader() {
	rf.role = Leader
	//rf.timeStart = time.Now()
	rf.timeOutPeriod = hbPeriod
}

func (rf *Raft) getLogSliceIdx(i int) int {
	return len(rf.log) - (int(rf.log[len(rf.log)-1].Idx) - i + 1)
}

func calculateElectionTimeOut() time.Duration {
	//old 150+150
	return time.Duration(rand.Int31n(300)) * time.Millisecond
}

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
