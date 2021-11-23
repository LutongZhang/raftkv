package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)


const (
	Query = 0
	Join = 1
	Leave = 2
	Move = 3

	//internlOp
	CurConfigUpdate = 4
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	cb       *pubSub
	currConfig int
	configs []Config // indexed by config num

	cliSeq *Cache
	make_end func(string) *labrpc.ClientEnd
}

type Snapshot struct {
	CurrConfig int
	Configs []Config
	CacheData map[uint32]int
}

type Op struct {
	Type int
	CbIdx uint32
	Args interface{}
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	output,err := sc.ProcessFunc(Join,args)
	if err != OK{
		reply.Err = err
		if err == ErrWrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*JoinReply)
		*reply = *v
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	output,err := sc.ProcessFunc(Leave,args)
	if err != OK{
		reply.Err = err
		if err == ErrWrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*LeaveReply)
		*reply = *v
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	output,err := sc.ProcessFunc(Move,args)
	if err != OK{
		reply.Err = err
		if err == ErrWrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*MoveReply)
		*reply = *v
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	output,err := sc.ProcessFunc(Query,args)
	if err != OK{
		reply.Err = err
		if err == ErrWrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*QueryReply)
		*reply = *v
	}
}

func (sc *ShardCtrler)ProcessFunc(Type int,args interface{})(output interface{},err Err){
	op := Op{
		Type,
		uuid.New().ID(),
		args,
	}
	ch :=sc.cb.subscribe(op.CbIdx)
	defer sc.cb.cancel(op.CbIdx)
	_,_,isLeader := sc.rf.Start(op)
	if !isLeader{
		return nil,ErrWrongLeader
	}
	select {
	case res :=<-ch:
		return res,OK
	case <-time.After(3*time.Second):
		return nil,ErrTimeOut
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//two phase commit method to update placement of shards in each raft group
//prepare phase: inform raft groups to retrieve needed shards
//commit phase: inform raft groups to delete shards they no longer own
//currConfig monotonically increasing，so committed is ok to commit again。shardMove also brought config version to prevent move error
func (sc *ShardCtrler)twoPhaseCommitShardMove(plan map[string]*ShardsMoveTask,oldConfig int,newConfig int){
	//phase 1 notify servers to get new shard
	var wg sync.WaitGroup
	for _,task := range plan{
		wg.Add(1)
		task := task
		go func() {
			defer wg.Done()
			sc.sendPrepareShardsMove(sc.getClients(task.toGroup),task)
		}()
	}
	wg.Wait()

	fromServers := make(map[int][]string)
	for _,task := range plan{
		if task.from == 0{
			continue
		}
		fromServers[task.from] = task.fromGroup
	}
	//phase2 notify servers to delete stale shard
	for _,group := range fromServers{
		wg.Add(1)
		group := group
		go func() {
			defer wg.Done()
			sc.sendCommitShardsMove(newConfig,sc.getClients(group))
		}()
	}
	wg.Wait()
	sc.ProcessFunc(CurConfigUpdate,[]int{oldConfig,newConfig})
}

//go routine periodically check if need shard move
//it s only the leader's job
func (sc *ShardCtrler)shardsUpdate(){
	for {
		_,isLeader:=sc.rf.GetState()
		if isLeader{
			sc.mu.Lock()
			currConfig := sc.currConfig
			latestConfig := len(sc.configs)-1
			if 	currConfig != latestConfig{
				plan := getMovePlan(&sc.configs[currConfig],&sc.configs[currConfig+1])
				sc.mu.Unlock()
				sc.twoPhaseCommitShardMove(plan,currConfig,currConfig+1)
			}else{
				sc.mu.Unlock()
			}
		}
		time.Sleep(time.Millisecond*50)
	}
}

//applier routine
func (sc *ShardCtrler)applier(){
	for msg := range sc.applyCh{
		if msg.CommandValid{
			op := msg.Command.(Op)
			sc.processOp(&op)
			if sc.rf.GetRaftStateSize() >= MaxRaftState && MaxRaftState != -1{//snapshot when raft state bigger then MaxRaftState
				sc.Snapshot(msg.CommandIndex)
			}
		}else if msg.SnapshotValid{
			ok := sc.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
			if ok{
				sc.restoreSnapshot(msg.Snapshot)
			}
		}
	}
}

func  (sc *ShardCtrler)processOp(op *Op){
	sc.mu.Lock()
	var reply interface{}
	switch op.Type {
	case Query:
		args :=op.Args.(*QueryArgs)
		config := *getConfig(sc.configs,args.Num)
		reply = &QueryReply{
			false,
			OK,
			config,
		}
	case Join:
		args :=op.Args.(*JoinArgs)
		r :=&JoinReply{}
		if sc.cliSeq.checkDup(args.CliId,args.SeqNum){
			r.WrongLeader = false
			r.Err = OK
			reply = r
		} else{
			newConfig := getConfig(sc.configs,-1).copy().AddNum().AddGroup(args.Servers).Rebalance()
			sc.configs = append(sc.configs,*newConfig)
			r.WrongLeader = false
			r.Err = OK
			reply = r
			sc.cliSeq.set(args.CliId,args.SeqNum)
		}
	case Leave:
		args :=op.Args.(*LeaveArgs)
		r :=&LeaveReply{}
		if sc.cliSeq.checkDup(args.CliId,args.SeqNum){
			r.WrongLeader = false
			r.Err = OK
			reply = r
		}else{
			newConfig := getConfig(sc.configs,-1).copy().AddNum().RmGroup(args.GIDs).Rebalance()
			sc.configs = append(sc.configs,*newConfig)
			r.WrongLeader = false
			r.Err = OK
			reply = r
			sc.cliSeq.set(args.CliId,args.SeqNum)
		}
	case Move:
		args :=op.Args.(*MoveArgs)
		r :=&MoveReply{}
		if sc.cliSeq.checkDup(args.CliId,args.SeqNum){
			r.WrongLeader = false
			r.Err = OK
			reply = r
		} else{
			newConfig := getConfig(sc.configs,-1).copy().MoveShard(args.Shard,args.GID)
			sc.configs = append(sc.configs,*newConfig)
			r.WrongLeader = false
			r.Err = OK
			reply = r
			sc.cliSeq.set(args.CliId,args.SeqNum)
		}
	case CurConfigUpdate:
		args := op.Args.([]int)
		if sc.currConfig == args[0]{
			sc.currConfig = args[1]
		}
	default:
		fmt.Println("unkown type for clter",op.Type)
	}

	sc.mu.Unlock()
	sc.cb.publish(op.CbIdx,reply)
}

func (sc *ShardCtrler)Snapshot(commandIdx int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	sc.mu.Lock()
	e.Encode(Snapshot{
		sc.currConfig,
		sc.configs,
		sc.cliSeq.Data,
	})
	sc.mu.Unlock()
	b := w.Bytes()
	sc.rf.Snapshot(commandIdx,b)
}

func (sc *ShardCtrler)restoreSnapshot(b []byte){
	if !(b == nil || len(b) < 1) {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		data := &Snapshot{}
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&data)
		sc.configs = data.Configs
		sc.currConfig = data.CurrConfig
		sc.cliSeq = newCache(5*time.Minute,data.CacheData)
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,make_end func(string) *labrpc.ClientEnd) *ShardCtrler {
	//rpc register structs
	labgob.Register(Op{})
	labgob.Register(&QueryArgs{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.mu = sync.RWMutex{}

	sc.configs = make([]Config, 1)
	sc.configs[0] = Config{
		0,
		[10]int{},
		map[int][]string{},
	}
	sc.currConfig = 0
	sc.make_end = make_end
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.cliSeq = newCache(5*time.Minute,make(map[uint32]int))
	sc.cb =  &pubSub{
		sync.RWMutex{},
		make(map[uint32]chan interface{}),
	}
	sc.restoreSnapshot(sc.rf.ReadSnapshot())
	go sc.applier()
	go sc.shardsUpdate()
	return sc
}
