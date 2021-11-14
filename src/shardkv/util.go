package shardkv

import (
	"6.824/labrpc"
	"fmt"
	"github.com/google/uuid"
	"reflect"
	"time"
)


func (kv *ShardKV)getClients(names []string)[]*labrpc.ClientEnd{
	res := []*labrpc.ClientEnd{}
	for _,name := range names{
		res = append(res, kv.make_end(name))
	}
	return res
}

func (kv *ShardKV)sendRetrieveShards(from int,fromGroup []string,shardsId []int)*RetrieveShardsReply{
	if from == 0{
		data := make(map[int]map[string]string)
		for _,v:= range shardsId{
			data[v]=make(map[string]string)
		}
		return &RetrieveShardsReply{
			OK,
			data,
			map[uint32]bool{},
		}
	}

	args := RetrieveShardsArgs{
		uuid.New().ID(),
		shardsId,
	}
	servers := kv.getClients(fromGroup)
	for {
		// try each known server.
		for _, srv := range servers {
			var reply RetrieveShardsReply
			ok := srv.Call("ShardKV.RetrieveShards", &args, &reply)
			if ok && reply.Err == OK{
				return &reply
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}



func getShardsInfo(shards  map[int]*Shard)[]string{
	info := []string{}
	for k,v:= range shards{
		info = append(info,fmt.Sprintf("%d: %s",k,v.State))
	}
	return info
}


func getErr(v interface{})Err{
	x:= reflect.Indirect(reflect.ValueOf(v)).FieldByName("Err").String()
	return Err(x)
}
