package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

const (
	Get  =  0
	Put = 1
	Append = 2
	ShardsAdd = 3
	RetrieveShards = 4
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	CbIdx uint32
	Args []byte
}

type Shard struct {
	State string
	Config int
	Data map[string]string
}

func(s *Shard)CopyData()map[string]string{
	data := make(map[string]string)
	for k,v := range s.Data{
		data[k] = v
	}
	return data
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	cb       *subPub
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck  *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	cliSeq *Cache
	shards map[int]*Shard
	// Your definitions here.
}


type Snapshot struct {
	Shards map[int]*Shard
	CacheData map[uint32]int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	output,err := kv.ProcessFunc(Get,args)
	if err != OK{
		reply.Err = err
	} else{
		v := output.(*GetReply)
		*reply = *v
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	Type := Put
	if args.Op == "Append"{
		Type = Append
	}
	output,err := kv.ProcessFunc(Type,args)
	if err != OK{
		reply.Err = err
	} else{
		v := output.(*PutAppendReply)
		*reply = *v
	}
}

//Todo 做version的比较
func (kv *ShardKV) PrepareShardMove(args *shardctrler.PrepareShardMoveArgs,reply *shardctrler.PrepareShardMoveReply) {
	if _,isLeader :=kv.rf.GetState();!isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	shards := []int{}
	for _,shardId := range args.ShardIds{
		shard,ok := kv.shards[shardId]
		if !(ok && shard.Config >= args.NewConfig){
			shards = append(shards,shardId)
		}
	}
	kv.mu.Unlock()
	if len(shards) == 0 {
		reply.Err = OK
		return
	}
	RetrievedShardsReply := kv.sendRetrieveShards(args.NewConfig,args.From,args.FromGroup,shards)
	if RetrievedShardsReply.Err == ErrOldConfig{
		reply.Err = OK
		return
	}

	shardsAddArgs :=  &ShardsMoveArgs{
		make(map[int]*Shard),
		RetrievedShardsReply.CliSeq,
	}
	for k,v := range RetrievedShardsReply.Data{
		shardsAddArgs.Data[k] = &Shard{
			Working,
			args.NewConfig,
			v,
		}
	}

	output,err:=kv.ProcessFunc(ShardsAdd,shardsAddArgs)
	if err != OK{
		reply.Err = shardctrler.Err(err)
	} else{
		v := output.(string)
		reply.Err = shardctrler.Err(v)
	}
}

func (kv *ShardKV)CommitShardMove(args *shardctrler.CommitShardArgs,reply *shardctrler.CommitShardReply)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//deleteExist := false
	for shardid,shard := range kv.shards{
		if shard.State==Stale && shard.Config<args.Config{
			delete(kv.shards,shardid)
			//deleteExist = true
		}
	}

	//delete some unused shards in snapshots by flush

	reply.Err = OK
}

func (kv *ShardKV)RetrieveShards(args *RetrieveShardsArgs,reply *RetrieveShardsReply){
	output,err := kv.ProcessFunc(RetrieveShards,args)
	if err != OK{
		reply.Err = err
	} else{
		v := output.(*RetrieveShardsReply)
		*reply = *v
	}
}

func (kv *ShardKV)ProcessFunc(Type int,args interface{})(output interface{},err Err){
	argsB,_ := json.Marshal(args)
	op := Op{
		Type,
		uuid.New().ID(),
		argsB,
	}
	ch :=kv.cb.subscribe(op.CbIdx)
	defer kv.cb.cancel(op.CbIdx)
	_,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		return nil,ErrWrongLeader
	}
	select {
	case res :=<-ch:
		return res,OK
	case <-time.After(3*time.Second):
		fmt.Println("timeout: ",args)
		return nil,ErrTimeOut
	}
}


func (kv *ShardKV)applier(){
	for msg := range kv.applyCh{
		if msg.CommandValid{
			op := msg.Command.(Op)
			kv.processOp(&op)
			if kv.rf.GetRaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1{
				//fmt.Println("xxxxx",kv.rf.GetRaftStateSize(),kv.rf.GetSnapShotSize())
				kv.Snapshot(msg.CommandIndex)
			}
		}else if msg.SnapshotValid{
			ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
			if ok{
				kv.restoreSnapshot(msg.Snapshot)
			}
		}
	}
}

func  (kv *ShardKV)processOp(op *Op){
	kv.mu.Lock()
	var reply interface{}
	switch op.Type {
	case Get:
		args := GetArgs{}
		json.Unmarshal(op.Args, &args)
		r := &GetReply{}
		if shard,ok := kv.shards[args.Shard];ok&&shard.State==Working{
			data := shard.Data
			if v,ok := data[args.Key];ok{
				r.Err = OK
				r.Value = v
			}else{
				r.Err = ErrNoKey
			}
		}else{
			r.Err = ErrWrongGroup
		}
		reply = r
		//if !kv.cliSeq.checkDup(args.CliId,args.SeqNum){
		//	kv.cliSeq.set(args.CliId,args.SeqNum)
		//}
	case Put:
		args := PutAppendArgs{}
		json.Unmarshal(op.Args, &args)
		r := &PutAppendReply{}
		if kv.cliSeq.checkDup(args.CliId,args.SeqNum){
			r.Err = OK
			reply = r
		}else {
			if shard,ok := kv.shards[args.Shard];ok&&shard.State==Working{
				data := shard.Data
				data[args.Key] = args.Value
				r.Err = OK
				kv.cliSeq.set(args.CliId,args.SeqNum)
			}else{
				r.Err = ErrWrongGroup
			}
			reply = r
		}
	case Append:
		args := PutAppendArgs{}
		json.Unmarshal(op.Args, &args)
		r := &PutAppendReply{}
		if kv.cliSeq.checkDup(args.CliId,args.SeqNum){
			r.Err = OK
			reply = r
		}else{
			if shard,ok := kv.shards[args.Shard];ok&&shard.State==Working{
				data := shard.Data
				if v,ok := data[args.Key];ok{
					data[args.Key] = v+args.Value
					r.Err = OK
					kv.cliSeq.set(args.CliId,args.SeqNum)
				}else{
					r.Err = ErrNoKey
				}
			}else{
				r.Err = ErrWrongGroup
			}
			reply = r
		}
	case ShardsAdd:
		args := ShardsMoveArgs{}
		json.Unmarshal(op.Args, &args)
		for shardid,newShard := range args.Data{
			v,ok :=kv.shards[shardid]
			if !ok ||(newShard.Config>v.Config){
				kv.shards[shardid] = newShard
			}
		}
		kv.cliSeq.combine(args.CliSeq)
		fmt.Println(fmt.Sprintf("shards add - gid: %d,me:%d,shards:%v",kv.gid,kv.me,getShardsInfo(kv.shards)))
		reply = OK
	case RetrieveShards:
		args := RetrieveShardsArgs{}
		json.Unmarshal(op.Args, &args)
		r := &RetrieveShardsReply{}
		r.Err = OK
		data := make(map[int]map[string]string)
		for _,shardId := range args.ShardsId{
			shard,ok := kv.shards[shardId]
			if !ok || shard.Config >=args.Config{
				r.Err = ErrOldConfig
			}else{
				kv.shards[shardId].State = Stale
				data[shardId] = kv.shards[shardId].CopyData()
			}
		}
		if r.Err == OK{
			r.Data = data
			r.CliSeq = kv.cliSeq.CopyData()
		}
		reply = r
		fmt.Println(fmt.Sprintf("shards be Retrieved - gid: %d,me:%d,shards:%v",kv.gid,kv.me,getShardsInfo(kv.shards)))
	default:
		fmt.Println("unknown type for kv:",op.Type)
	}

	kv.mu.Unlock()
	kv.cb.publish(op.CbIdx,reply)
}

func (kv *ShardKV)Snapshot(commandIdx int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(Snapshot{
		kv.shards,
				kv.cliSeq.Data,
	})
	kv.mu.Unlock()
	b := w.Bytes()
	kv.rf.Snapshot(commandIdx,b)
}

func (kv *ShardKV)restoreSnapshot(b []byte){
	if !(b == nil || len(b) < 1) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		data := &Snapshot{}
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&data)
		kv.shards = data.Shards
		kv.cliSeq = newCache(5*time.Minute,data.CacheData)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}




//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.mu = sync.RWMutex{}
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.cliSeq = newCache(5*time.Minute,make(map[uint32]int))
	kv.cb =  &subPub{
		sync.RWMutex{},
		make(map[uint32]chan interface{}),
	}
	kv.shards = make(map[int]*Shard)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.restoreSnapshot(kv.rf.ReadSnapshot())
	go kv.applier()

	return kv
}
