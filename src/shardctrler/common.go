package shardctrler

import (
	"math"
	"sort"
	"sync"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

//MaxRaftState
const MaxRaftState = 1000

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (cfg *Config)copy()*Config{
	shards := [10]int{}
	groups := make(map[int][]string)
	for i:=0;i<len(cfg.Shards);i++{
		shards[i] = cfg.Shards[i]
	}
	for k,v := range cfg.Groups{
		var s []string
		copy(v,s)
		groups[k] = v
	}
	newConfig := &Config{
		cfg.Num,
		shards,
		groups,
	}
	return newConfig
}

func (cfg *Config)AddNum()*Config{
	cfg.Num+=1
	return cfg
}

func (cfg *Config)AddGroup(Servers map[int][]string)*Config{
	for k,v := range Servers{
		cfg.Groups[k] = v
	}
	return cfg
}

func (cfg *Config)RmGroup(GIDs []int)*Config{
	for _,gid := range GIDs {
		delete(cfg.Groups,gid)
	}
	return cfg
}

func (cfg *Config)MoveShard(shard int, gid int)*Config{
	cfg.Shards[shard] = gid
	return cfg
}

func (cfg *Config)Rebalance()*Config{
	gids := make([]int, len(cfg.Groups))
	i := 0
	for k := range cfg.Groups {
		gids[i] = k
		i++
	}
	sort.Ints(gids)
	divValue := int(math.Floor((float64(NShards)/float64(len(gids)) + 0.5)))
	for i,v := range gids{
		for j:=0;j < divValue;j++{
			if divValue*i+j >= len(cfg.Shards){
				break
			}
			cfg.Shards[divValue*i+j] = v
		}
	}
	return cfg
}

//Err type
const (
	OK = "OK"
	ErrTimeOut = "Err Time Out"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	UUID uint32
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	UUID uint32
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	UUID uint32
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	UUID uint32
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

//
type PrepareShardMoveArgs struct {
	UUID uint32
	To int
	ToGroup []string
	From int
	FromGroup []string
	ShardIds []int
}

type PrepareShardMoveReply struct {
	Err Err
}

type CommitShardArgs struct {
	UUID  uint32
}

type CommitShardReply struct {
	Err Err
}
//

type ShardsMoveTask struct {
	to int
	toGroup []string
	from int
	fromGroup []string
	shardIds []int
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
