package raft

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int64
	Idx     int64
	//Committed bool
	//Applied   bool
}

type Role int

const (
	Leader = iota
	Follower
	Candidate
)

//const hbTimeOut = time.Millisecond * 1500
//const hbPeriod = time.Millisecond * 500

//TOdo use context to stop routine

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

	} else {
		rf.role = Follower
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.dead = 0
		rf.commitIdx = 0
		rf.lastApplied = 0
		rf.nextIdx = make([]int64, 0, 0)
		rf.matchIdx = make([]int64, 0, 0)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx = append(rf.nextIdx, 1)
			rf.matchIdx = append(rf.matchIdx, 0)
		}
		rf.setNewElectionPoint()
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int64
	CandidateId int
	LastLogIdx  int64
	LastLogTerm int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term      int64
	VoteGrant bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		return
	}

	//fmt.Println(fmt.Sprintf("%d with term %d to %d with term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term))
	if args.Term > rf.currentTerm {
		rf.changeToFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
		//rf.persist()
	}

	lastLog := rf.log[len(rf.log)-1]
	uptoData := (args.LastLogTerm == lastLog.Term && args.LastLogIdx >= lastLog.Idx) || (args.LastLogTerm > lastLog.Term)
	if rf.votedFor == -1 && uptoData {
		rf.votedFor = args.CandidateId
		rf.changeToFollower()
		reply.Term = rf.currentTerm
		reply.VoteGrant = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//Todo use election task
func (rf *Raft) startElection() {
	ch := make(chan bool)
	defer close(ch)
	rf.mu.Lock()
	//rf.role = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	currTerm := rf.currentTerm
	me := rf.me
	lastLog := rf.log[len(rf.log)-1]
	rf.mu.Unlock()
	fmt.Println(fmt.Sprintf("%d start election with term %d ", rf.me, rf.currentTerm))

	recVotes := 1
	finish := false

	for idx, _ := range rf.peers {
		idx := idx
		if idx == rf.me {
			continue
		}
		go func() {
			args := &RequestVoteArgs{
				currTerm,
				me,
				lastLog.Idx,
				lastLog.Term,
			}
			reply := &RequestVoteReply{}
			//todo term to higher
			ok := rf.sendRequestVote(idx, args, reply)
			if ok && reply.VoteGrant {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				recVotes += 1
				if recVotes > len(rf.peers)/2 && !finish {
					finish = true
					rf.changeToLeader()
					go rf.replicateLogs()
					fmt.Println(fmt.Sprintf("%d becomes leader with term %d", rf.me, rf.currentTerm))
				}
			}
		}()
	}
}

//AppendEntry
type AppendEntryArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64 //leader's commit index
}

type AppendEntryReply struct {
	Term    int64
	Success bool
}

//TOdo 调整时间 ,防止重复复制,Todo 看commit的时候参考现在的log
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println(fmt.Sprintf("%s %d receive appendEntry from %d", roleStr(rf.role), rf.me, args.LeaderId))
	//reset term
	if args.Term >= rf.currentTerm {
		rf.changeToFollower()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//fmt.Println("rec Append Entry with curr commit idx", rf.commitIdx)
	if rf.log[len(rf.log)-1].Idx < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	for len(rf.log) > 0 && rf.log[len(rf.log)-1].Idx > args.PrevLogIndex {
		rf.log = rf.log[:len(rf.log)-1]
	}
	if args.PrevLogTerm == rf.log[len(rf.log)-1].Term {
		reply.Term = rf.currentTerm
		reply.Success = true
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		fmt.Println(fmt.Sprintf("%s %d append %v in log with commitIdx %d lastApplied %d", roleStr(rf.role), rf.me, rf.log[len(rf.log)-1], rf.commitIdx, rf.lastApplied))
		//progress commitIdx //
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, rf.log[len(rf.log)-1].Idx)
			rf.sendLogsToApplier()
		}
		return
	} else {
		rf.log = rf.log[:len(rf.log)-1]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}

//AppendEntries
func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
	if rf.getRoleL() != Leader {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLog := rf.log[len(rf.log)-1]
	newIdx := prevLog.Idx + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
		Idx:     newIdx,
	})
	fmt.Println(fmt.Sprintf("%s %d append %v in log with commitIdx %d lastApplied %d", roleStr(rf.role), rf.me, rf.log[len(rf.log)-1], rf.commitIdx, rf.lastApplied))
	rf.replicateLogs()
	return int(newIdx), int(term), true
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
//todo timeout 可不可以不用
//TOdo 发生了太多次term增加
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		electionTimePoint := rf.electioTimePoint
		me := rf.me
		rf.mu.Unlock()
		if role == Leader {
			rf.replicateLogs()
		} else {
			if time.Now().After(electionTimePoint) {
				if role == Follower {
					fmt.Println(fmt.Sprintf("%s %d heartbeat timeout", roleStr(role), me))
					rf.mu.Lock()
					rf.changeToCandidate()
					rf.mu.Unlock()
					rf.startElection()
				} else if role == Candidate {
					rf.mu.Lock()
					rf.setNewElectionPoint()
					rf.mu.Unlock()
					rf.startElection()
				}
			}
		}
		//fmt.Println(fmt.Sprintf("%d sleep %s",rf.me,rf.getTimeOutL()))
		//timeOut = rf.getTimeOutL()
		//time.Sleep(timeOut)
		time.Sleep(50 * time.Millisecond)
	}
}

//logworker receive command from channel
func (rf *Raft) replicateLogs() {
	//fmt.Println("start replicate with commit idx", rf.commitIdx)
	for idx, _ := range rf.peers {
		idx := idx
		if idx == rf.me {
			continue
		}
		go rf.replicateLog(idx)
	}
}

func (rf *Raft) replicateLog(peerIdx int) {
	rf.mu.Lock()
	role := rf.role

	if role != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	me := rf.me
	commitIdx := rf.commitIdx
	//lastIdx := rf.log[len(rf.log)-1].Idx
	peerNextIdx := rf.nextIdx[peerIdx]
	//Todo 针对hb再处理，optimize
	//if peerNextIdx > rf.log[len(rf.log)-1].Idx {
	//	rf.mu.Unlock()
	//	return
	//}
	var entries []LogEntry
	i := rf.getLogSliceIdx(int(peerNextIdx))
	if i < len(rf.log) {
		entries = rf.log[i:len(rf.log)]
	}
	//fmt.Println("prev log: ", rf.nextIdx)
	prevLog := rf.log[i-1]
	rf.mu.Unlock()
	args := &AppendEntryArgs{
		Term:         term,
		LeaderId:     me,
		PrevLogIndex: prevLog.Idx,
		PrevLogTerm:  prevLog.Term,
		LeaderCommit: commitIdx,
		Entries:      entries,
	}
	reply := &AppendEntryReply{}
	ok := rf.SendAppendEntry(peerIdx, args, reply)
	rf.ProcessAppendEntryReply(peerIdx, ok, args, reply)
}

func (rf *Raft) ProcessAppendEntryReply(peerIdx int, ok bool, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//
	if !ok {
		go rf.replicateLog(peerIdx)
		return
	}

	if reply.Success {
		newNextIdx := args.PrevLogIndex + int64(len(args.Entries)) + 1
		newMatchIdx := args.PrevLogIndex + int64(len(args.Entries))
		if newNextIdx > rf.nextIdx[peerIdx] {
			rf.nextIdx[peerIdx] = newNextIdx
		}
		if newMatchIdx > rf.matchIdx[peerIdx] {
			rf.matchIdx[peerIdx] = newMatchIdx
		}
		rf.leaderCommitLogs()
		rf.sendLogsToApplier()
	} else {
		if reply.Term > rf.currentTerm {
			rf.changeToFollower()
			return
		}
		//for condition: reply term > old term, but not new term
		if !(reply.Term > args.Term) {
			rf.nextIdx[peerIdx] -= 1
		}
		//if rf.nextIdx[peerIdx]
		go rf.replicateLog(peerIdx)
	}
}

//
func (rf *Raft) leaderCommitLogs() {
	if rf.role != Leader {
		return
	}
	//TODO match里面最小超过n/2的
	for i := len(rf.log) - 1; i >= 0; i-- {
		logEntry := rf.log[i]
		matches := 1
		for _, matchIdx := range rf.matchIdx {
			if matchIdx >= logEntry.Idx {
				matches++
			}
			if matches > len(rf.peers)/2 {
				break
			}
		}
		if matches > len(rf.peers)/2 {
			if logEntry.Term != rf.currentTerm || rf.commitIdx >= logEntry.Idx {
				break
			}
			rf.commitIdx = logEntry.Idx
			fmt.Println(fmt.Sprintf("%s %d commit %s", roleStr(rf.role), rf.me, rf.log[i].Command))
		}
	}
}

func (rf *Raft) sendLogsToApplier() {
	i := rf.getLogSliceIdx(int(rf.lastApplied)) + 1
	j := rf.getLogSliceIdx(int(rf.commitIdx))
	for i <= j {
		rf.applierCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: int(rf.log[i].Idx),
		}
		if rf.log[i].Idx > rf.lastApplied {
			rf.lastApplied = rf.log[i].Idx
		}
		fmt.Println(fmt.Sprintf("%s %d apply %s with commitIdx %d, lastApplied %d", roleStr(rf.role), rf.me, rf.log[i].Command, rf.commitIdx, rf.lastApplied))
		i++
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applierCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
