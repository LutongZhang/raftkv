package shardkv

import "sync"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//



const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeOut"
)

//shard state
const (
	Working = "Working"
	Stale = "Stale"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	UUID  uint32
	Key   string
	Shard int
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	UUID uint32
	Key string
	Shard int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

//type PrepareShardMoveArgs struct {
//	UUID uint32
//	To int
//	ToGroup []string
//	From int
//	FromGroup []string
//	Shards []int
//}
//
//type PrepareShardMoveReply struct {
//	Err Err
//}
//
//type CommitShardArgs struct {
//	UUID  uint32
//}
//
//type CommitShardReply struct {
//	Err Err
//}

type RetrieveShardsArgs struct {
	UUID  uint32
	Shards []int
}

type RetrieveShardsReply struct {
	Err Err
	Data map[int]map[string]string
}

type subPub struct {
	mu      sync.RWMutex
	mem map[uint32]chan interface{}
}

func (sp *subPub)subscribe(key uint32)chan interface{}{
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.mem[key] = make(chan interface{},1)
	return sp.mem[key]
}

func (sp *subPub)publish(key uint32,res interface{}){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	ch,ok := sp.mem[key]
	if ok{
		ch <- res
	}
}

func (sp *subPub)cancel(key uint32){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.mem,key)
}

