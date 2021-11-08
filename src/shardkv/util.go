package shardkv

import (
	"6.824/labrpc"
	"github.com/google/uuid"
	"time"
)


func (kv *ShardKV)getClients(names []string)[]*labrpc.ClientEnd{
	res := []*labrpc.ClientEnd{}
	for _,name := range names{
		res = append(res, kv.make_end(name))
	}
	return res
}

func (kv *ShardKV)sendRetrieveShards(from int,fromGroup []string,shards[]int)map[int]map[string]string{
	if from == 0{
		data := make(map[int]map[string]string)
		for _,v:= range shards{
			data[v]=make(map[string]string)
		}
		return data
	}

	args := RetrieveShardsArgs{
		uuid.New().ID(),
		shards,
	}
	servers := kv.getClients(fromGroup)
	for {
		// try each known server.
		for _, srv := range servers {
			var reply RetrieveShardsReply
			ok := srv.Call("ShardKV.RetrieveShards", &args, &reply)
			if ok && reply.Err == OK{
				return reply.Data
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}