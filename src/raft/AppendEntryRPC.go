package raft

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

//logworker receive command from channel
func (rf *Raft) replicateLogs() {
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

	var entries []LogEntry
	var prevLog LogEntry
	if rf.nextIdx[peerIdx] <= rf.log[0].Idx {
		rf.nextIdx[peerIdx] = rf.log[0].Idx + 1
	} else if rf.nextIdx[peerIdx]-1 > rf.log[len(rf.log)-1].Idx {
		rf.nextIdx[peerIdx] = rf.log[len(rf.log)-1].Idx
	}
	i := getLogSliceIdx(rf.log, int(rf.nextIdx[peerIdx]))
	//peer catch up, maybe hb condition
	if i < len(rf.log) {
		entries = rf.log[i:]
		//entries = make([]LogEntry,len(rf.log[i:]))
		//copy(entries,rf.log[i:])
	}
	prevLog = rf.log[i-1]
	rf.mu.Unlock()
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLog.Idx,
		PrevLogTerm:  prevLog.Term,
		LeaderCommit: rf.commitIdx,
		Entries:      entries,
	}
	reply := &AppendEntryReply{}
	ok := rf.SendAppendEntry(peerIdx, args, reply)
	rf.ProcessAppendEntryReply(peerIdx, ok, args, reply)
}

//AppendEntries
func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//TOdo 调整时间 ,防止重复复制,Todo 看commit的时候参考现在的log
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//rf.log_infof("append %v in %v",args.Entries,rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.changeToFollower(args.Term, -1)
		} else {
			rf.changeToFollower(rf.currentTerm, rf.votedFor)
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.log[len(rf.log)-1].Idx < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//不能直接用args.entry覆盖logs，防止有网络延迟，旧的请求把新请求添加的删除
	var ptr int
	if args.PrevLogIndex >= rf.log[0].Idx {
		ptr = getLogSliceIdx(rf.log, int(args.PrevLogIndex))
	} else {
		args.PrevLogIndex = rf.log[0].Idx
		args.PrevLogTerm = rf.log[0].Term
		ptr = 0
		if args.Entries != nil && args.Entries[len(args.Entries)-1].Idx > args.PrevLogIndex {
			i := getLogSliceIdx(args.Entries, int(args.PrevLogIndex))
			args.Entries = args.Entries[i+1:]
		} else {
			args.Entries = nil
		}
	}
	if args.PrevLogTerm == rf.log[ptr].Term {
		reply.Term = rf.currentTerm
		reply.Success = true
		ptr++
		for j := 0; j < len(args.Entries); j++ {
			if ptr < len(rf.log) && rf.log[ptr].Idx == args.Entries[j].Idx && rf.log[ptr].Term == args.Entries[j].Term {
				ptr++
			} else {
				rf.log = rf.log[:ptr]
				rf.appendLogs(args.Entries[j:])
				rf.log_infof("append %v from %d",args.Entries[j:], args.LeaderId)
				break
			}
		}
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, rf.log[len(rf.log)-1].Idx)
			rf.signalApplier()
		}
		return
	} else {
		rf.log = rf.log[:ptr]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
}

func (rf *Raft) ProcessAppendEntryReply(peerIdx int, ok bool, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		go rf.replicateLog(peerIdx)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term, -1)
		return
	} else if rf.currentTerm == args.Term{
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
		} else {
			//for condition: reply term > old term, but not new term
			//&& rf.nextIdx[peerIdx] > 1 or rf.next[peerIdx] = args.prevLogIdx
			if !(reply.Term > args.Term) && rf.nextIdx[peerIdx] > args.PrevLogIndex {
				rf.nextIdx[peerIdx] -= 1
			}
			go rf.replicateLog(peerIdx)
		}
	}
}

//
func (rf *Raft) leaderCommitLogs() {
	//TODO match里面最小超过n/2的
	if rf.role != Leader {
		return
	}
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
			if logEntry.Term == rf.currentTerm && logEntry.Idx > rf.commitIdx {
				rf.commitIdx = logEntry.Idx
				rf.signalApplier()
				rf.log_infof("commit %v",rf.log[i])
			}
			break
		}
	}
}
