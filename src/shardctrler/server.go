package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"encoding/json"
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

//Todo 2 phase commit to update shards, apply to rg if rg config num < taget num

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	cb       *subPub
	currConfig int
	// Your data here.
	configs []Config // indexed by config num

	cache *Cache
	make_end func(string) *labrpc.ClientEnd
}

type Op struct {
	// Your data here.
	Type int
	CbIdx uint32
	Args []byte
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	output,err := sc.ProcessFunc(Join,args.UUID,args)
	if err != OK{
		reply.Err = err
		if err == ErrWrongLeader{
			reply.WrongLeader = true
		}
	} else{
		//fmt.Println(output,err)
		v := output.(*JoinReply)
		*reply = *v
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	output,err := sc.ProcessFunc(Leave,args.UUID,args)
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
	output,err := sc.ProcessFunc(Move,args.UUID,args)
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
	// Your code here.
	output,err := sc.ProcessFunc(Query,args.UUID,args)
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

func (sc *ShardCtrler)ProcessFunc(Type int,uuid uint32,args interface{})(output interface{},err Err){
	argsB,_ := json.Marshal(args)
	op := Op{
		Type,
		uuid,
		argsB,
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

func (sc *ShardCtrler)twoPhaseCommitShardMove(plan map[string]*ShardsMoveTask,oldConfig int,newConfig int){
	//phase 1 notify servers to get new shard
	fmt.Println(fmt.Sprintf("config %d -> %d Plan: %v",oldConfig,newConfig,plan))
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
	fmt.Println("phase 1 complete")
	//
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
			sc.sendCommitShardsMove(sc.getClients(group))
		}()
	}
	wg.Wait()
	fmt.Println("phase 2 complete")
	sc.ProcessFunc(CurConfigUpdate,uuid.New().ID(),[]int{oldConfig,newConfig})
}


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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//

func (sc *ShardCtrler)applier(){
	for msg := range sc.applyCh{
		if msg.CommandValid{
			op := msg.Command.(Op)
			sc.processOp(&op)
			if sc.rf.GetRaftStateSize() >= MaxRaftState && MaxRaftState != -1{
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
		//sc.mu.Lock()
		args := QueryArgs{}
		json.Unmarshal(op.Args, &args)
		config := *getConfig(sc.configs,args.Num)
		reply = &QueryReply{
			false,
			OK,
			config,
		}
		//sc.mu.Unlock()
	case Join:
		//sc.mu.Lock()
		args := JoinArgs{}
		json.Unmarshal(op.Args, &args)
		r :=&JoinReply{}
		if sc.cache.get(args.UUID){
			r.WrongLeader = false
			r.Err = OK
			reply = r
			//sc.mu.Lock()
			goto cb
		}
		newConfig := getConfig(sc.configs,-1).copy().AddNum().AddGroup(args.Servers).Rebalance()
		sc.configs = append(sc.configs,*newConfig)
		r.WrongLeader = false
		r.Err = OK
		reply = r
		sc.cache.set(args.UUID)
		//fmt.Println(sc.configs)
		//sc.mu.Unlock()
	case Leave:
		//sc.mu.Lock()
		args := LeaveArgs{}
		json.Unmarshal(op.Args, &args)
		r :=&LeaveReply{}
		if sc.cache.get(args.UUID){
			r.WrongLeader = false
			r.Err = OK
			reply = r
			//sc.mu.Lock()
			goto cb
		}
		newConfig := getConfig(sc.configs,-1).copy().AddNum().RmGroup(args.GIDs).Rebalance()
		sc.configs = append(sc.configs,*newConfig)
		r.WrongLeader = false
		r.Err = OK
		reply = r
		sc.cache.set(args.UUID)
		//sc.mu.Unlock()
	case Move:
		//sc.mu.Lock()
		args := MoveArgs{}
		json.Unmarshal(op.Args, &args)
		r :=&MoveReply{}
		if sc.cache.get(args.UUID){
			r.WrongLeader = false
			r.Err = OK
			reply = r
			//sc.mu.Lock()
			goto cb
		}
		newConfig := getConfig(sc.configs,-1).copy().MoveShard(args.Shard,args.GID)
		sc.configs = append(sc.configs,*newConfig)
		r.WrongLeader = false
		r.Err = OK
		reply = r
		sc.cache.set(args.UUID)
		//sc.mu.Unlock()
	case CurConfigUpdate:
		//sc.mu.Lock()
		args := []int{}
		json.Unmarshal(op.Args, &args)
		if sc.currConfig == args[0]{
			sc.currConfig = args[1]
		}
		fmt.Println(fmt.Sprintf("CurrConfigUpdate - config: %d,Content:%v",sc.currConfig,sc.configs[sc.currConfig].Shards))
		//sc.mu.Unlock()
	default:
		fmt.Println("unkown type for clter",op.Type)
	}
	//q.Println("ctrler op:",op.Type,reply)
cb:
	sc.mu.Unlock()
	sc.cb.publish(op.CbIdx,reply)
}

type Snapshot struct {
	CurrConfig int
	Configs []Config
	CacheData map[uint32]bool
}

func (sc *ShardCtrler)Snapshot(commandIdx int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	sc.mu.Lock()
	e.Encode(Snapshot{
		sc.currConfig,
		sc.configs,
		sc.cache.Data,
	})
	sc.mu.Unlock()
	b := w.Bytes()
	sc.rf.Snapshot(commandIdx,b)
}

func (sc *ShardCtrler)restoreSnapshot(b []byte){
	if !(b == nil || len(b) < 1) {
		//fmt.Println(fmt.Sprintf("%d snapshot restore",kv.me))
		sc.mu.Lock()
		defer sc.mu.Unlock()
		data := &Snapshot{}
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&data)
		sc.configs = data.Configs
		sc.currConfig = data.CurrConfig
		sc.cache = newCache(5*time.Minute,data.CacheData)
	}
}
//Todo 加入一个make_end Func
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,make_end func(string) *labrpc.ClientEnd) *ShardCtrler {
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

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.cache = newCache(5*time.Minute,make(map[uint32]bool))
	sc.cb =  &subPub{
		sync.RWMutex{},
		make(map[uint32]chan interface{}),
	}
	// Your code here.
	sc.restoreSnapshot(sc.rf.ReadSnapshot())
	go sc.applier()
	go sc.shardsUpdate()
	return sc
}
