package shardctrler

//
// Shardctrler clerk.
//
//Todo remove replicate

import (
	"6.824/labrpc"
	"github.com/google/uuid"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	CliId uint32
	SeqNum int
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	//ck.SeqNum+=1
	args := &QueryArgs{
		ck.CliId,
		//ck.SeqNum,
		num,
	}
	// Your code here.
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

func (ck *Clerk) Join(servers map[int][]string) {
	ck.SeqNum+=1
	args := &JoinArgs{
		ck.CliId,
		ck.SeqNum,
		servers,
	}
	// Your code here.
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

func (ck *Clerk) Leave(gids []int) {
	ck.SeqNum+=1
	args := &LeaveArgs{
		ck.CliId,
		ck.SeqNum,
		gids,
	}
	// Your code here.
	args.GIDs = gids

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

func (ck *Clerk) Move(shard int, gid int) {
	ck.SeqNum+=1
	args := &MoveArgs{
		ck.CliId,
		ck.SeqNum,
		shard,
		gid,
	}
	// Your code here.
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
