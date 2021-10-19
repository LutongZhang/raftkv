package raft

type InstallSnapshotArgs struct {
	Term            int64
	LeaderId        int
	LastIncludeIdx  int64
	LastIncludeTerm int64
	Data            []byte
}

type InstallSnapshotReply struct {
	Term int64
}

func (rf *Raft) installSnapshotToPeers(lastIncludedIndex int64, lastIncludedTerm int64, data []byte) {
	for idx, _ := range rf.peers {
		idx := idx
		if idx == rf.me {
			continue
		}
		go rf.installSnapshotToPeer(idx, lastIncludedIndex, lastIncludedTerm, data)
	}
}

func (rf *Raft) installSnapshotToPeer(peer int, lastIncludedIndex int64, lastIncludedTerm int64, data []byte) {
	rf.mu.Lock()
	if lastIncludedIndex < rf.log[0].Idx {
		rf.mu.Unlock()
		return
	}
	currTerm := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()
	args := &InstallSnapshotArgs{
		currTerm,
		leaderId,
		lastIncludedIndex,
		lastIncludedTerm,
		data,
	}
	reply := &InstallSnapshotReply{}
	ok := rf.SendInstallSnapshot(peer, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term, -1)
		}
	}
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}
	currTerm := rf.currentTerm
	rf.mu.Unlock()
	rf.applierCh <- ApplyMsg{
		SnapshotValid: true,
		SnapshotTerm:  int(args.LastIncludeTerm),
		SnapshotIndex: int(args.LastIncludeIdx),
		Snapshot:      args.Data,
	}
	reply.Term = currTerm
	return
}
