package raft

import (
	"6.824/labgob"
	"bytes"
	"os"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	Leader = iota
	Follower
	Candidate
)

//TOdo use context to stop routine
type LogEntry struct {
	Command interface{}
	Term    int64
	Idx     int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	role Role
	me   int   // this peer's index into peers[]
	dead int32 // set by Kill()

	//Persist
	currentTerm int64
	votedFor    int
	log         []LogEntry
	//Log
	commitIdx   int64
	lastApplied int64

	//Leader State
	nextIdx  []int64
	matchIdx []int64

	//ticker
	electioTimePoint time.Time

	//Applier
	applierCh chan ApplyMsg
	applyCond *sync.Cond

	//snapshot
	//snapshot []byte
	//snapshotIdx int64
	//snapshotTerm int64
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.role = Follower
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.dead = 0
		rf.log = []LogEntry{
			{
				nil,
				rf.currentTerm,
				0,
				//true,
				//true,
			},
		}
		rf.commitIdx = 0
		rf.lastApplied = 0
		rf.nextIdx = make([]int64, 0, 0)
		rf.matchIdx = make([]int64, 0, 0)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx = append(rf.nextIdx, 1)
			rf.matchIdx = append(rf.matchIdx, 0)
		}
		rf.setNewElectionPoint()
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int64
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		rf.log_info("Decode persisted raft state wrong")
		os.Exit(-1)
	} else {
		//Todo put the first log as last include idx term, directly update nextidx lastApplied?
		rf.role = Follower
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.dead = 0
		rf.commitIdx = 0
		rf.lastApplied = rf.log[0].Idx
		rf.nextIdx = make([]int64, 0, 0)
		rf.matchIdx = make([]int64, 0, 0)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx = append(rf.nextIdx, rf.log[0].Idx+1)
			rf.matchIdx = append(rf.matchIdx, 0)
		}
		rf.setNewElectionPoint()
	}
}

func (rf *Raft) signalApplier() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied+1 <= rf.commitIdx {
			i := getLogSliceIdx(rf.log, int(rf.lastApplied)) + 1
			//rf.log_infof("apply log %v",rf.log[i])
			rf.lastApplied+=1
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: int(rf.log[i].Idx),
			}
			rf.mu.Unlock()
			rf.applierCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		electionTimePoint := rf.electioTimePoint
		if role == Leader {
			rf.replicateLogs()
		} else if time.Now().After(electionTimePoint) {
			rf.changeToCandidate(rf.currentTerm+1, rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
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
	rf.mu = sync.RWMutex{}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applierCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	///TODO 暂时
	rf.initLogger()

	return rf
}
