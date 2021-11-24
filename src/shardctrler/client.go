package shardctrler

//
// Shardctrler clerk.

import (
	"crypto/rand"
	"github.com/LutongZhang/rgkv/src/labrpc"
	"github.com/google/uuid"
	"math/big"
	"time"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	CliId uint32 //identifier
	SeqNum int //cmds are sent Synchronously,each with seqNum
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.CliId = uuid.New().ID()
	ck.SeqNum = 0
	return ck
}

// Query config with version, -1 is the latest version
func (ck *Clerk) Query(num int) Config {
	//query is Idempotent，not need seq
	args := &QueryArgs{
		ck.CliId,
		num,
	}
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Join new raft group
func (ck *Clerk) Join(servers map[int][]string) {
	ck.SeqNum+=1
	args := &JoinArgs{
		ck.CliId,
		ck.SeqNum,
		servers,
	}
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave raft group
func (ck *Clerk) Leave(gids []int) {
	ck.SeqNum+=1
	args := &LeaveArgs{
		ck.CliId,
		ck.SeqNum,
		gids,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//Move a shard to certain raft group
func (ck *Clerk) Move(shard int, gid int) {
	ck.SeqNum+=1
	args := &MoveArgs{
		ck.CliId,
		ck.SeqNum,
		shard,
		gid,
	}
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
