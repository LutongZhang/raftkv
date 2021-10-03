package raft

import "fmt"

//Todo use election task
func (rf *Raft) startElection() {
	ch := make(chan bool)
	defer close(ch)
	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	currTerm := rf.currentTerm
	me := rf.me
	lastLog := rf.log[len(rf.log)-1]
	fmt.Println(fmt.Sprintf("%d start election with term %d ", rf.me, rf.currentTerm))
	rf.mu.Unlock()
	//
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
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGrant {
					recVotes += 1
					if recVotes > len(rf.peers)/2 && !finish {
						finish = true
						//Todo
						//检查和当前term是否匹配，防止网络延迟，旧的term同意leader，但是新的term已经有新的leader，造成脑裂
						if rf.currentTerm != currTerm {
							return
						}
						rf.changeToLeader()
						go rf.replicateLogs()
						fmt.Println(fmt.Sprintf("%d becomes leader with term %d", rf.me, rf.currentTerm))
					}
				} else if reply.Term > rf.currentTerm {
					rf.changeToFollower(reply.Term, -1)
				}
			}
		}()
	}
}

type RequestVoteArgs struct {
	Term        int64
	CandidateId int
	LastLogIdx  int64
	LastLogTerm int64
}

type RequestVoteReply struct {
	Term      int64
	VoteGrant bool
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
//sender
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//receiver
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}

	lastLog := rf.log[len(rf.log)-1]
	uptoData := (args.LastLogTerm == lastLog.Term && args.LastLogIdx >= lastLog.Idx) || (args.LastLogTerm > lastLog.Term)
	if rf.votedFor == -1 && uptoData {
		rf.changeToFollower(rf.currentTerm, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGrant = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGrant = false
	}
}
