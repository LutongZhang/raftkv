package raft


func (rf *Raft) startElection() {
	if rf.role != Candidate {
		return
	}
	currTerm := rf.currentTerm
	me := rf.me
	lastLog := rf.log[len(rf.log)-1]
	rf.log_info("start election")
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
			ok := rf.sendRequestVote(idx, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGrant {
					recVotes += 1
					//become leader if majority agree
					if recVotes > len(rf.peers)/2 && !finish {
						finish = true
						//current term must be same as election term! prevent split brain
						if rf.currentTerm != currTerm {
							return
						}
						rf.changeToLeader()
						//inform peers I am the leader
						rf.replicateLogs()
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

//rpc call
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote receive vote from other candidate
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
	//vote requirement
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
