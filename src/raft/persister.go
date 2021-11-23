package raft

//
// support for Raft and kv to save persistent
// Raft state (log &c) and k/v server snapshots.

import (
	"6.824/labgob"
	"bytes"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

//Persist util
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persistState() {
	currTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currTerm)
	e.Encode(votedFor)
	e.Encode(log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistStateL() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persistState()
}

func (rf *Raft) persistStateAndSnapshot(snapshotByte []byte) {
	currTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currTerm)
	e.Encode(votedFor)
	e.Encode(log)
	stateByte := w.Bytes()
	rf.persister.SaveStateAndSnapshot(stateByte, snapshotByte)
}

func (rf *Raft) persistStateAndSnapshotL(snapshotByte []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persistStateAndSnapshot(snapshotByte)
}
