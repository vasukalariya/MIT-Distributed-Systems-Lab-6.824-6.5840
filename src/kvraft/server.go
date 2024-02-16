package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"bytes"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Opr       string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore    map[string]string
	dupTable   map[int64]Op
	responseCh map[int]chan Op
	lastApplied int
}

func (kv *KVServer) lastOperation(client int64) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op, ok := kv.dupTable[client]; ok {
		return op.RequestId
	}
	return -1

}

func (kv *KVServer) fetchDupEntry(clientId int64, requestId int, key string) (bool, string) {
	op, ok := kv.dupTable[clientId]
	if !ok || op.RequestId < requestId{
		return false, ""
	}
	return true, op.Value
}

func (kv *KVServer) checkChannel(index int) chan Op{
	_, ok := kv.responseCh[index]
	if !ok {
		kv.responseCh[index] = make(chan Op, 1)
	}
	return kv.responseCh[index]
}

func (kv *KVServer) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId && 
			a.RequestId == b.RequestId && 
			a.Key == b.Key && 
			a.Opr == b.Opr
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if kv.lastOperation(args.ClientId) >= args.RequestId {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.kvStore[args.Key]
		reply.Err = OK
		DPrintf("[%d] GET_DUP client [%d] requestid [%d] index []", kv.me, args.ClientId, args.RequestId)
		return
	}

	command := Op{
		Key:       args.Key,
		Value:     "",
		Opr:       "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// DPrintf("[%d] INIT_GET client [%d] requestid [%d] index []", kv.me, args.ClientId, args.RequestId)

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.checkChannel(index)
	kv.mu.Unlock()

	DPrintf("[%d] GET client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	select {
	case op := <-ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = op.Value
		reply.Err = OK
		DPrintf("[%d] GET_COMPLETED client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Err = ErrWrongLeader
		DPrintf("[%d] TIMEOUT_GET client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.lastOperation(args.ClientId) >= args.RequestId {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = OK
		DPrintf("[%d] PUT_DUP client [%d] requestid [%d] index []", kv.me, args.ClientId, args.RequestId)
		return
	}

	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Opr:       args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// DPrintf("[%d] INIT_PUT client [%d] requestid [%d] index []", kv.me, args.ClientId, args.RequestId)

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.checkChannel(index)
	kv.mu.Unlock()

	DPrintf("[%d] PUT client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	
	select {
	case op := <- ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		DPrintf("[%d] PUT_COMPLETED client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	case <-time.After(time.Duration(300) * time.Millisecond):
		DPrintf("[%d] TIMEOUT_PUT client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
		reply.Err = ErrWrongLeader
	}

}




func (kv *KVServer) callSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvStore)
		e.Encode(kv.dupTable)

		if err := e.Encode(kv.kvStore); err != nil {
			DPrintf("Error in encoding kvStore")
		}
		if err := e.Encode(kv.dupTable); err != nil {
			DPrintf("Error in encoding dupTable")
		}

		kv_state := w.Bytes()

		kv.rf.Snapshot(kv.lastApplied, kv_state)
		DPrintf("[%d] SNAPSHOT client [] requestid [] index [%d]", kv.me, kv.lastApplied)
	}
}

func (kv *KVServer) restoreState(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)


	var tmpKvStore map[string]string
	var tmpDupTable map[int64]Op

	if d.Decode(&tmpKvStore) != nil ||
		d.Decode(&tmpDupTable) != nil {
	   DPrintf("Error in restoring state!")
	} else {
		kv.kvStore = tmpKvStore
		kv.dupTable = tmpDupTable
	}
	
}


func (kv *KVServer) receiveUpdates() {

	for response := range kv.applyCh {
		DPrintf("[%d] RECEIVED client [] requestid [] index [%d]", kv.me, response.CommandIndex)
		if response.CommandValid {
			cmd := response.Command.(Op)
			index := response.CommandIndex
			k := cmd.Key
			v := cmd.Value
			op := cmd.Opr
			clientId := cmd.ClientId
			requestId := cmd.RequestId

			DPrintf("[%d] RESPONSE client [%d] requestid [%d] index [%d]", kv.me, clientId, requestId, index)
			kv.mu.Lock()
			if kv.lastApplied >= index {
				kv.mu.Unlock()
				continue
			}

			ch := kv.checkChannel(index)
			ok, val := kv.fetchDupEntry(clientId, requestId, k)
			if ok {
				cmd.Value = val
			} else {
				if op == "Get" {
					cmd.Value = kv.kvStore[k]
					kv.dupTable[clientId] = cmd
				} else if op == "Append" {
					kv.kvStore[k] += v
					cmd.Value = kv.kvStore[k]
					kv.dupTable[clientId] = cmd
				} else if op == "Put" {
					kv.kvStore[k] = v
					cmd.Value = kv.kvStore[k]
					kv.dupTable[clientId] = cmd
				}
			}
			kv.lastApplied = index
			DPrintf("[%d] APPLIED client [%d] requestid [%d] index [%d]", kv.me, clientId, requestId, index)
			kv.mu.Unlock()
			ch <- cmd
		}
		if response.SnapshotValid {
			kv.restoreState(response.Snapshot)
			kv.mu.Lock()
			kv.lastApplied = response.SnapshotIndex
			kv.mu.Unlock()
		}
		kv.callSnapshot()
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	// kv.opCh = make(map[int]chan Op)
	kv.responseCh = make(map[int]chan Op)
	kv.dupTable = make(map[int64]Op)
	kv.lastApplied = 0

	kv.restoreState(persister.ReadSnapshot())

	go kv.receiveUpdates()

	return kv
}
