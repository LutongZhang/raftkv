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

func (rf *Raft) resetTerm(newTerm int64){
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.role = Follower
	rf.heartBeatTime = time.Now()
	rf.timeOutPeriod = time.Duration(rand.Int31n(150)+150) * time.Millisecond
}

func (rf *Raft)resetHeatBeat(){
	rf.heartBeatTime = time.Now()
	rf.timeOutPeriod = time.Duration(rand.Int31n(150)+150) * time.Millisecond
}