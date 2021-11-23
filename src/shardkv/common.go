package shardkv

import "sync"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//


const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeOut"
	ErrOldConfig = "ErrWrongConfig"
)

//shard state
const (
	Working = "Working"
	Stale = "Stale"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	CliId uint32
	SeqNum int
	Key   string
	Shard int
	Value string
	Op    string // "Put" or "Append"
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	CliId uint32
	Key string
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type RetrieveShardsArgs struct {
	Config int
	ShardsId []int
}

type RetrieveShardsReply struct {
	Err Err
	Data map[int]map[string]string
	CliSeq map[uint32]int
}

type ShardsMoveArgs struct {
	Data map[int]*Shard
	CliSeq map[uint32]int
}

type ShardsDeleteArgs struct {
	CommitConfig int
}

type pubSub struct {
	mu      sync.RWMutex
	mem map[uint32]chan interface{}
}

func (sp *pubSub)subscribe(key uint32)chan interface{}{
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.mem[key] = make(chan interface{},1)
	return sp.mem[key]
}

func (sp *pubSub)publish(key uint32,res interface{}){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	ch,ok := sp.mem[key]
	if ok{
		ch <- res
	}
}

func (sp *pubSub)cancel(key uint32){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.mem,key)
}

