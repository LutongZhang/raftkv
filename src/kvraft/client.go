package kvraft

import (
	"6.824/labrpc"
	"fmt"
	rand2 "math/rand"
	"reflect"
	"sync"
)
import "crypto/rand"
import "math/big"
//Todo 用read+set做幂等，提高吞吐

type MSG struct {
	args interface{}
	reply interface{}
	cb chan bool
}

type Clerk struct {
	mu sync.RWMutex
	servers []*labrpc.ClientEnd
	leader int
	sessionId int64
	serialNum int
	msgChan chan MSG
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
	ck.msgChan = make(chan MSG)
	go ck.RPCWorker()
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
	//ck.serialNum+=1
	args :=&GetArgs{
		Key: key,
	}
	reply := &GetReply{}
	cb := make(chan bool)
	ck.msgChan <-MSG{args,reply,cb}
	<-cb
	return reply.Value
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
	msg := MSG{
		&PutAppendArgs{
			Key: key,
			Value: value,
			Op: op,
		},
		&PutAppendReply{},
		make(chan bool),
	}
	ck.msgChan <-msg
	<-msg.cb
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk)RPCWorker(){
	for msg := range ck.msgChan{
		ck.serialNum+=1
		args := msg.args
		reply := msg.reply
		switch args.(type) {
		case *GetArgs:
			args.(*GetArgs).SessionId = ck.sessionId
			args.(*GetArgs).SerialNum = ck.serialNum
			ck.sendRPC("KVServer.Get",args,reply)
			msg.cb <-true
		case *PutAppendArgs:
			args.(*PutAppendArgs).SessionId = ck.sessionId
			args.(*PutAppendArgs).SerialNum = ck.serialNum
			ck.sendRPC("KVServer.PutAppend",args,reply)
			msg.cb<-true
		default:
			fmt.Println("unknown")
			msg.cb<-false
		}
	}
}

func (ck *Clerk)sendRPC(command string,args interface{},reply interface{}){
	for true {
		err :=ck.sendRPCtoL(command,args,reply)
		if err == OK{
			return
		}
	}
}

func (ck *Clerk)sendRPCtoL(command string,args interface{},reply interface{})Err{
	l := ck.leader
	var err Err
	for true{
		ok := ck.servers[l].Call(command,args,reply)
		if ok{
			err = getErr(reply)
			if err!= ErrWrongLeader{
				if ck.leader != l{
					ck.leader = l
				}
				break
			}
		}
		l = (l+1)%len(ck.servers)
	}
	return err
}

func getErr(v interface{})Err{
	x:= reflect.Indirect(reflect.ValueOf(v)).FieldByName("Err").String()
	return Err(x)
}
