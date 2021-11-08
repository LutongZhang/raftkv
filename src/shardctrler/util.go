package shardctrler

import (
	"6.824/labrpc"
	"fmt"
	"github.com/google/uuid"
	"time"
)

func (sc *ShardCtrler)getClients(names []string)[]*labrpc.ClientEnd{
	res := []*labrpc.ClientEnd{}
	for _,name := range names{
		res = append(res, sc.make_end(name))
	}
	return res
}

func (sc *ShardCtrler)sendPrepareShardsMove(servers []*labrpc.ClientEnd,task *ShardsMoveTask){
	args := PrepareShardMoveArgs{
		uuid.New().ID(),
		task.to,
		task.toGroup,
		task.from,
		task.fromGroup,
		task.shards,
	}
	for {
		// try each known server.
		for _, srv := range servers {
			var reply PrepareShardMoveReply
			ok := srv.Call("ShardKV.PrepareShardMove", &args, &reply)
			if ok && reply.Err == OK{
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (sc *ShardCtrler)sendCommitShardsMove(servers []*labrpc.ClientEnd){
	args := CommitShardArgs{
		uuid.New().ID(),
	}
	for {
		// try each known server.
		for _, srv := range servers {
			var reply CommitShardReply
			ok := srv.Call("ShardKV.CommitShardMove", &args, &reply)
			if ok && reply.Err == OK{
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func getConfig(cfgs []Config,num int)*Config{
	if num == -1 || num >= len(cfgs){
		return &cfgs[len(cfgs)-1]
	} else{
		return &cfgs[num]
	}
}

func getMovePlan(old *Config,new *Config)map[string]*ShardsMoveTask{
	plan := make(map[string]*ShardsMoveTask)
	oldshards := old.Shards
	newshards := new.Shards
	for i,oldShardRG := range oldshards{
		newShardRG := newshards[i]
		if oldShardRG != newShardRG{
			k :=fmt.Sprintf("%d,%d",oldShardRG,newShardRG)
			 if _,ok := plan[k];!ok{
			 	fromGroup := []string{}
			 	if group,ok := old.Groups[oldShardRG];ok{
			 		fromGroup = group
				}
			 	plan[k] = &ShardsMoveTask{
			 		newShardRG,
			 		new.Groups[newShardRG],
			 		oldShardRG,
			 		fromGroup,
			 		[]int{i},
				}
			 } else{
			 	plan[k].shards =append(plan[k].shards,i)
			 }
		}
	}
	return plan
}