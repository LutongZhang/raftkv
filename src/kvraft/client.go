package kvraft

import (
	"6.824/labrpc"
	rand2 "math/rand"
	"reflect"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	mu sync.RWMutex
	servers []*labrpc.ClientEnd
	leader int
	sessionId int64
	serialNum int
	// You will have to modify this struct.
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
	ck.leader = 0
	ck.sessionId = rand2.Int63()
	ck.serialNum = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.serialNum+=1
	for true{
		args := GetArgs{
			ck.sessionId,
			ck.serialNum,
			key,
		}
		reply := GetReply{}
		ck.sendRPCtoL("KVServer.Get",&args,&reply)
		if reply.Err == OK{
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.serialNum+=1
	for true {
		args := PutAppendArgs{
			ck.sessionId,
			ck.serialNum,
			key,
			value,
			op,
		}
		reply := PutAppendReply{}
		ck.sendRPCtoL("KVServer.PutAppend",&args,&reply)
		if reply.Err == OK{
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk)sendRPCtoL(command string,args interface{},reply interface{}){

	l := ck.leader
	for true{
		ok := ck.servers[l].Call(command,args,reply)
		if ok{
			if getErr(reply)!= ErrWrongLeader{
				if ck.leader != l{
					ck.leader = l
				}
				break
			}
		}
		l = (l+1)%len(ck.servers)
	}
}

func getErr(v interface{})Err{
	x:= reflect.Indirect(reflect.ValueOf(v)).FieldByName("Err").String()
	return Err(x)
}
