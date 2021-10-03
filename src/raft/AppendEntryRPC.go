package raft

import "fmt"

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
	peerNextIdx := rf.nextIdx[peerIdx]
	lastIncludedIdx := rf.log[0].Idx
	var entries []LogEntry
	var prevLog LogEntry
	if peerNextIdx <= lastIncludedIdx {
		rf.nextIdx[peerIdx] = lastIncludedIdx + 1
		peerNextIdx = lastIncludedIdx + 1
	}
	i := getLogSliceIdx(rf.log, int(peerNextIdx))
	//peer catch up, maybe hb condition
	if i < len(rf.log) {
		entries = rf.log[i:]
	}
	prevLog = rf.log[i-1]

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

//TOdo 调整时间 ,防止重复复制,Todo 看commit的时候参考现在的log
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(fmt.Sprintf("%s %d receive appendEntry from %d", roleStr(rf.role), rf.me, args.LeaderId))
	//reset term
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
				fmt.Println(fmt.Sprintf("%s %d append %v from %d in log %v with commitIdx %d lastApplied %d", roleStr(rf.role), rf.me, args.Entries[j:], args.LeaderId, rf.log, rf.commitIdx, rf.lastApplied))
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
		//fmt.Println("yyyy", rf.me, args.LeaderId, args.PrevLogIndex)
		return
	}
}

//AppendEntries
func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) ProcessAppendEntryReply(peerIdx int, ok bool, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}
	//fmt.Println(rf.me, "append Entry for", peerIdx)
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
		rf.signalApplier()
	} else {
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(rf.currentTerm, rf.votedFor)
			return
		}
		//for condition: reply term > old term, but not new term
		//&& rf.nextIdx[peerIdx] > 1 or rf.next[peerIdx] = args.prevLogIdx
		if !(reply.Term > args.Term) && rf.nextIdx[peerIdx] > args.PrevLogIndex {
			rf.nextIdx[peerIdx] -= 1
			//fmt.Println("xxxxx", rf.me, peerIdx, rf.nextIdx[peerIdx])
		}
		//if rf.nextIdx[peerIdx]
		go rf.replicateLog(peerIdx)
	}
}

//
func (rf *Raft) leaderCommitLogs() {
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
			fmt.Println(fmt.Sprintf("%s %d commit %v", roleStr(rf.role), rf.me, rf.log[i]))
		}
	}
}
