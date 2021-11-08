package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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

	shardsState map[int]string
	shards map[int]map[string]string
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	output,err := kv.ProcessFunc(Get,args.UUID,args)
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
	output,err := kv.ProcessFunc(Type,args.UUID,args)
	if err != OK{
		reply.Err = err
	} else{
		v := output.(*PutAppendReply)
		*reply = *v
	}
}

//Todo 做version的比较
func (kv *ShardKV) PrepareShardMove(args *shardctrler.PrepareShardMoveArgs,reply *shardctrler.PrepareShardMoveReply) {
	kv.mu.Lock()
	shards := []int{}
	for _,shard := range args.Shards{
		state,ok := kv.shardsState[shard]
		if !(ok && state == Working){
			shards = append(shards,shard)
		}
	}
	kv.mu.Unlock()
	if len(shards) == 0 {
		reply.Err = OK
		return
	}
	data := kv.sendRetrieveShards(args.From,args.FromGroup,shards)
	output,err:=kv.ProcessFunc(ShardsAdd,uuid.New().ID(),data)
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
	for shard,state := range kv.shardsState{
		if state ==Stale{
			delete(kv.shardsState,shard)
			delete(kv.shards,shard)
		}
	}
	reply.Err = OK

}

func (kv *ShardKV)RetrieveShards(args *RetrieveShardsArgs,reply *RetrieveShardsReply){
	output,err := kv.ProcessFunc(RetrieveShards,args.UUID,args)
	if err != OK{
		reply.Err = err
	} else{
		v := output.(*RetrieveShardsReply)
		*reply = *v
	}
}

func (kv *ShardKV)ProcessFunc(Type int,uuid uint32,args interface{})(output interface{},err Err){
	argsB,_ := json.Marshal(args)
	op := Op{
		Type,
		uuid,
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
		return nil,ErrTimeOut
	}
}


func (kv *ShardKV)applier(){
	for msg := range kv.applyCh{
		if msg.CommandValid{
			op := msg.Command.(Op)
			kv.processOp(&op)
			if kv.rf.GetRaftStateSize() >= kv.maxraftstate && kv.maxraftstate != -1{
				//kv.Snapshot(msg.CommandIndex)
			}
		}else if msg.SnapshotValid{
			ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
			if ok{
				//kv.restoreSnapshot(msg.Snapshot)
			}
		}
	}
}

func  (kv *ShardKV)processOp(op *Op){
	var reply interface{}
	switch op.Type {
	case Get:
		kv.mu.Lock()
		args := GetArgs{}
		json.Unmarshal(op.Args, &args)
		r := &GetReply{}
		if v,ok := kv.shardsState[args.Shard];ok&&v==Working{
			shardStore := kv.shards[args.Shard]
			if v,ok := shardStore[args.Key];ok{
				r.Err = OK
				r.Value = v
			}else{
				r.Err = ErrNoKey
			}
		}else{
			r.Err = ErrWrongGroup
		}
		reply = r
		kv.mu.Unlock()
	case Put:
		kv.mu.Lock()
		args := PutAppendArgs{}
		json.Unmarshal(op.Args, &args)
		r := &PutAppendReply{}
		if v,ok := kv.shardsState[args.Shard];ok&&v==Working{
			shardStore := kv.shards[args.Shard]
			shardStore[args.Key] = args.Value
			r.Err = OK
		}else{
			r.Err = ErrWrongGroup
		}
		reply = r
		kv.mu.Unlock()
	case Append:
		kv.mu.Lock()
		args := PutAppendArgs{}
		json.Unmarshal(op.Args, &args)
		r := &PutAppendReply{}
		if v,ok := kv.shardsState[args.Shard];ok&&v==Working{
			shardStore := kv.shards[args.Shard]
			if v,ok := shardStore[args.Key];ok{
				r.Err = OK
				shardStore[args.Key] = v+args.Value
			}else{
				r.Err = ErrNoKey
			}
		}else{
			r.Err = ErrWrongGroup
		}
		reply = r
		kv.mu.Unlock()
	case ShardsAdd:
		kv.mu.Lock()
		args := map[int]map[string]string{}
		json.Unmarshal(op.Args, &args)
		r := OK
		for k,v := range args{
			kv.shardsState[k] = Working
			kv.shards[k] = v
		}
		reply = r
		fmt.Println(fmt.Sprintf("shardsAdd- gid: %d,me:%d,shards:%v",kv.gid,kv.me,kv.shardsState))
		kv.mu.Unlock()
	case RetrieveShards:
		kv.mu.Lock()
		args := RetrieveShardsArgs{}
		json.Unmarshal(op.Args, &args)
		data := make(map[int]map[string]string)
		for _,shard := range args.Shards{
			kv.shardsState[shard] = Stale
			data[shard] = kv.shards[shard]
		}
		reply = &RetrieveShardsReply{
			OK,
			data,
		}
		fmt.Println(fmt.Sprintf("Retrieve shards - gid: %d,me:%d,shards:%v",kv.gid,kv.me,kv.shardsState))
		kv.mu.Unlock()
	}
	kv.cb.publish(op.CbIdx,reply)
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
	kv.cb =  &subPub{
		sync.RWMutex{},
		make(map[uint32]chan interface{}),
	}
	kv.shardsState = map[int]string{}
	kv.shards = make(map[int]map[string]string)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()

	return kv
}
