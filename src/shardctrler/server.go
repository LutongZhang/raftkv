package shardctrler


import (
	"6.824/raft"
	"github.com/google/uuid"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	Query = 0
	Join = 1
	Leave = 2
	Move = 3
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	cb       subPub //Todo 用idx作为key
	// Your data here.
	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type int
	cbIdx uint32
	Args interface{}
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	output,err := sc.ProcessFunc(Query,args)
	if err != OK{
		reply.Err = err
		if err == WrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*JoinReply)
		reply = v
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	output,err := sc.ProcessFunc(Query,args)
	if err != OK{
		reply.Err = err
		if err == WrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*LeaveReply)
		reply = v
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	output,err := sc.ProcessFunc(Query,args)
	if err != OK{
		reply.Err = err
		if err == WrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*MoveReply)
		reply = v
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	output,err := sc.ProcessFunc(Query,args)
	if err != OK{
		reply.Err = err
		if err == WrongLeader{
			reply.WrongLeader = true
		}
	} else{
		v := output.(*QueryReply)
		reply = v
	}

}

func (sc *ShardCtrler)ProcessFunc(Type int,args interface{})(output interface{},err Err){
	op := Op{
		Type,
		uuid.New().ID(),
		args,
	}
	ch :=sc.cb.subscribe(op.cbIdx)
	defer sc.cb.cancel(op.cbIdx)
	_,_,isLeader := sc.rf.Start(op)
	if !isLeader{
		return nil,WrongLeader
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
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}


func (sc *ShardCtrler)applier(){
	for msg := range sc.applyCh{
		if msg.CommandValid{
			//fmt.Println(fmt.Sprintf("%d apply command",kv.me))
			op := msg.Command.(Op)
			//fmt.Println(fmt.Sprintf("%d op: %v",kv.me,op))
			sc.processOp(&op)
			if sc.rf.GetRaftStateSize() >= MaxRaftState && MaxRaftState != -1{
				//fmt.Println(fmt.Sprintf("%d snapshot start",kv.me))
				sc.Snapshot(msg.CommandIndex)
			}
		}else if msg.SnapshotValid{
			//fmt.Println(fmt.Sprintf("%d apply snapshot",kv.me))
			ok := sc.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
			if ok{
				sc.restoreSnapshot(msg.Snapshot)
			}
		}
	}
}

func  (sc *ShardCtrler)processOp(op *Op){
	var reply interface{}
	switch op.Type {
	case Query:
		sc.mu.Lock()
		args := op.Args.(QueryArgs)
		config := *getConfig(sc.configs,args.Num)
		reply = &QueryReply{
			false,
			OK,
			config,
		}
		sc.mu.Unlock()
	case Join:
		sc.mu.Lock()
		args := op.Args.(JoinArgs)
		newConfig := getConfig(sc.configs,-1).copy().AddNum().AddGroup(args.Servers).Rebalance()
		sc.configs = append(sc.configs,*newConfig)
		sc.mu.Unlock()
	case Leave:
		sc.mu.Lock()
		args := op.Args.(LeaveArgs)
		newConfig := getConfig(sc.configs,-1).copy().AddNum().RmGroup(args.GIDs).Rebalance()
		sc.configs = append(sc.configs,*newConfig)
		sc.mu.Unlock()
	case Move:
		sc.mu.Lock()
		args := op.Args.(MoveArgs)
		newConfig := getConfig(sc.configs,-1).copy().MoveShard(args.Shard,args.GID)
		sc.configs = append(sc.configs,*newConfig)
		sc.mu.Unlock()
	}
	sc.cb.publish(op.cbIdx,reply)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.mu = sync.RWMutex{}

	sc.configs = make([]Config, 1)
	sc.configs[0] = Config{
		0,
		[10]int{},
		map[int][]string{},
	}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.cb =  subPub{
		sync.RWMutex{},
		make(map[uint32]chan interface{}),
	}
	// Your code here.

	go sc.applier()

	return sc
}
