package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func uniKey(sessionId int64,serialNum int)string{
	return strconv.Itoa(int(sessionId)) + "&" + strconv.Itoa(serialNum)
}

type Snapshot struct {
	Sessions   map[int64]int
	Store 		map[string]string
}

type Op struct {
	SessionId  int64
	SerialNum int
	OpType string
	Key string
	Value string
}

type subPub struct {
	mu      sync.RWMutex
	mem map[string]chan *Op
}

func (sp *subPub)subscribe(key string)chan *Op{
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.mem[key] = make(chan *Op,1)
	return sp.mem[key]
}

func (sp *subPub)publish(key string,res *Op){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	ch,ok := sp.mem[key]
	if ok{
		ch <- res
	}
}

func (sp *subPub)cancel(key string){
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.mem,key)
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	store 		map[string]string
	sessions   map[int64]int  //需要恢复
	cb       subPub //Todo 用idx作为key
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//fmt.Println("start: ",uniKey(args.SessionId,args.SerialNum))
	Key := uniKey(args.SessionId,args.SerialNum)
	ch := kv.cb.subscribe(Key)
	_,_,isLeader := kv.rf.Start(Op{
		args.SessionId,
		args.SerialNum,
		"Get",
		args.Key,
		"",
	})
	if !isLeader{
		reply.Err = ErrWrongLeader
		goto release
		return
	}
	select {
		case res :=<-ch:
			reply.Err = OK
			reply.Value = res.Value
		case <-time.After(3*time.Second):
			reply.Err = ErrTimeOut
	}

release:
	kv.cb.cancel(Key)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Println("start: ",uniKey(args.SessionId,args.SerialNum))
	kv.mu.Lock()
	did := false
	lastSerialNum,ok :=  kv.sessions[args.SessionId]
	if ok {
		did = args.SerialNum <= lastSerialNum
	}
	kv.mu.Unlock()
	if did{
		reply.Err = OK
		return
	}
	key := uniKey(args.SessionId,args.SerialNum)
	ch := kv.cb.subscribe(key)
	_,_,isLeader := kv.rf.Start(Op{
		args.SessionId,
		args.SerialNum,
		args.Op,
		args.Key,
		args.Value,
	})
	if !isLeader{
		reply.Err = ErrWrongLeader
		goto release
		return
	}
	//fmt.Println("start: ",uniKey(args.SessionId,args.SerialNum))
	select {
		case <-ch:
			reply.Err = OK
		case <-time.After(3*time.Second):
			reply.Err = ErrTimeOut
	}

release:
	kv.cb.cancel(key)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func  (kv *KVServer)processOp(op *Op){
	switch op.OpType {
		case "Get":
			c, ok := kv.store[op.Key]
			if ok {
				op.Value = c
			}
		case "Put":
				kv.mu.Lock()
				v,ok := kv.sessions[op.SessionId]
				if !ok ||(ok && op.SerialNum > v){
					kv.sessions[op.SessionId] = op.SerialNum
					kv.store[op.Key] = op.Value
				}
				kv.mu.Unlock()
		case "Append":
			kv.mu.Lock()
			v,ok := kv.sessions[op.SessionId]
			if !ok ||(ok && op.SerialNum > v){
				kv.sessions[op.SessionId] = op.SerialNum
				kv.store[op.Key] += op.Value
			}
			kv.mu.Unlock()
	}
	kv.cb.publish(uniKey(op.SessionId,op.SerialNum),op)
}

func (kv *KVServer)applier(){
	for msg := range kv.applyCh{
		if msg.CommandValid{
			//fmt.Println(fmt.Sprintf("%d apply command",kv.me))
			op := msg.Command.(Op)
			kv.processOp(&op)
			if kv.rf.GetRaftStateSize() >= kv.maxraftstate{
				//fmt.Println(fmt.Sprintf("%d snapshot start",kv.me))
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(Snapshot{
					kv.sessions,
					kv.store,
				})
				b := w.Bytes()
				kv.rf.Snapshot(msg.CommandIndex,b)
			}
		}else if msg.SnapshotValid{
			//fmt.Println(fmt.Sprintf("%d apply snapshot",kv.me))
			ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm,msg.SnapshotIndex,msg.Snapshot)
			if ok{
				kv.restoreSnapshot(msg.Snapshot)
			}
		}
	}
}

func (kv *KVServer)restoreSnapshot(b []byte){
	if !(b == nil || len(b) < 1) {
		//fmt.Println(fmt.Sprintf("%d snapshot restore",kv.me))
		data := &Snapshot{}
		r := bytes.NewBuffer(b)
		d := labgob.NewDecoder(r)
		d.Decode(&data)
		kv.store = data.Store
		kv.sessions = data.Sessions
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.mu = sync.RWMutex{}
	kv.maxraftstate = maxraftstate
	kv.store = make(map[string]string)
	kv.sessions = make(map[int64]int)
	kv.cb = subPub{
		sync.RWMutex{},
			make(map[string]chan *Op),
	}
	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.restoreSnapshot(kv.rf.ReadSnapshot())

	go kv.applier()
	// You may need initialization code here.

	return kv
}