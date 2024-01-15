package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Op string
	Node int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore map[string]string
	putAppCh chan bool
	getCh chan string
}

func (kv *KVServer) checkLeader() bool {

	_, isLeader := kv.rf.GetState()	

	return isLeader
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	command := Command{
		Key: args.Key,
		Value: "",
		Op: "Get",
	}

	DPrintf("[%d] Adding get [%s] on raft ", kv.me, args.Key)
	kv.rf.Start(command)

	reply.Value = <- kv.getCh
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	command := Command{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
	}


	DPrintf("[%d] Adding (%s) [%s] [%s] on raft ", kv.me, args.Op, args.Key, args.Value)
	kv.rf.Start(command)

	<- kv.putAppCh
	reply.Err = OK
}


func (kv *KVServer) receiveUpdates() {
	for {
		response := <- kv.applyCh
		if response.CommandValid {
			kv.mu.Lock()
			cmd := response.Command.(Command)
			k := cmd.Key
			v := cmd.Value
			op := cmd.Op
			DPrintf("[%d] Update Received {%s | %s | %s}", kv.me, k, v, op)
			if op == "Get" {
				val := kv.kvStore[k]
				kv.mu.Unlock()
				kv.getCh <- val
				continue
			}

			if op == "Append" {
				val := kv.kvStore[k]
				val += v
				kv.kvStore[k] = val
			} else if op == "Put" {
				kv.kvStore[k] = v
			} 
			kv.mu.Unlock()
			kv.putAppCh <- true
		}
	}
}



// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.putAppCh = make(chan bool)
	kv.getCh = make(chan string)

	go kv.receiveUpdates()

	return kv
}
