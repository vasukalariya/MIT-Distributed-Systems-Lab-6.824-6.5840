package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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


type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
	
	kvStore    map[int]map[string]string // shard -> key-value
	dupTable   map[int64]int // clientId -> requestId
	
	responseCh map[int]chan Op 
	lastApplied int

	serveShards map[int]string // shard -> status

	mck *shardctrler.Clerk
}



func (kv *ShardKV) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Opr == b.Opr
}


func (kv *ShardKV) checkDup(clientId int64, requestId int) bool {
	reqId, ok := kv.dupTable[clientId]
	if !ok || reqId < requestId {
		return false
	}
	return true
}

func (kv *ShardKV) checkChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.responseCh[index]
	if !ok {
		kv.responseCh[index] = make(chan Op, 1)
	}
	return kv.responseCh[index]
}


func (kv *ShardKV) handleKey(key string) bool {
	shard := key2shard(key)
	if val, ok := kv.serveShards[shard]; !ok || val != "serve" {
		DPrintf("[%d %d] WRONG_GROUP key [%s] shard [%d] SS %v", kv.gid, kv.me, key, shard, kv.serveShards)
		return false
	}
	return true
}





func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.handleKey(args.Key) {
		DPrintf("\n\n[%d %d] WRONG_GROUP client [%d] requestid [%d] shard [%d]\n\n", kv.gid, kv.me, args.ClientId, args.RequestId, key2shard(args.Key))
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)

	isDup := kv.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.Value = kv.kvStore[shard][args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		Key:       args.Key,
		Value:     "",
		Opr:       "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.checkChannel(index)

	// DPrintf("[%d] GET client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	select {
	case op := <-ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = op.Value
		reply.Err = OK
		// DPrintf("[%d] GET_COMPLETED client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Err = ErrWrongLeader
		// DPrintf("[%d] TIMEOUT_GET client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	}

}






func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.handleKey(args.Key) {
		DPrintf("\n\n[%d %d] WRONG_GROUP client [%d] requestid [%d] shard [%d]\n\n", kv.gid, kv.me, args.ClientId, args.RequestId, key2shard(args.Key))
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	isDup := kv.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()


	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Opr:       args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.checkChannel(index)

	// DPrintf("[%d] PUT client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	select {
	case op := <-ch:
		if !kv.sameOps(op, command) {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
		// DPrintf("[%d] PUT_COMPLETED client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	case <-time.After(time.Duration(300) * time.Millisecond):
		reply.Err = ErrWrongLeader
		// DPrintf("[%d] TIMEOUT_GET client [%d] requestid [%d] index [%d]", kv.me, args.ClientId, args.RequestId, index)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *ShardKV) callSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvStore)
		e.Encode(kv.dupTable)
		e.Encode(kv.currentConfig)
		e.Encode(kv.serveShards)

		if err := e.Encode(kv.kvStore); err != nil {
			DPrintf("[%d %d] Error in encoding kvStore", kv.gid, kv.me)
			return
		}
		if err := e.Encode(kv.dupTable); err != nil {
			DPrintf("[%d %d] Error in encoding dupTable", kv.gid, kv.me)
			return
		}
		if err := e.Encode(kv.currentConfig); err != nil {
			DPrintf("[%d %d] Error in encoding currentConfig", kv.gid, kv.me)
			return
		}
		if err := e.Encode(kv.serveShards); err != nil {
			DPrintf("[%d %d] Error in encoding serveShards", kv.gid, kv.me)
			return
		}

		kv_state := w.Bytes()

		kv.rf.Snapshot(kv.lastApplied, kv_state)
		DPrintf("[%d] SNAPSHOT client [] requestid [] index [%d]", kv.me, kv.lastApplied)
	}
}




func (kv *ShardKV) restoreState(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var tmpKvStore map[int]map[string]string
	var tmpDupTable map[int64]int
	var tmpCurrentConfig shardctrler.Config
	var tmpServeShards map[int]string

	if d.Decode(&tmpKvStore) != nil ||
		d.Decode(&tmpDupTable) != nil || 
		d.Decode(&tmpCurrentConfig) != nil ||
		d.Decode(&tmpServeShards) != nil {
	   DPrintf("[%d %d] Error in restoring state!", kv.gid, kv.me)
	} else {
		kv.kvStore = tmpKvStore
		kv.dupTable = tmpDupTable
		kv.currentConfig = tmpCurrentConfig
		kv.serveShards = tmpServeShards
		kv.lastConfig = kv.mck.Query(kv.currentConfig.Num-1)
	}

}




func (kv *ShardKV) receiveUpdates() {

	for response := range kv.applyCh {
		// DPrintf("[%d] RECEIVED client [] requestid [] index [%d]", kv.me, response.CommandIndex)
		if response.CommandValid {
			index := response.CommandIndex
			kv.mu.Lock()
			
			if index <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = index
			kv.mu.Unlock()

			if cfg, ok := response.Command.(shardctrler.Config); ok {
				
				DPrintf("[%d %d] RECEIVED CONFIG %d %v", kv.gid, kv.me, cfg.Num, cfg.Shards)
				kv.handleConfigChange(cfg)

			} else if reply, ok := response.Command.(MigrateReply); ok {
				
				DPrintf("[%d %d] MIGRATION RECEIVED %v", kv.gid, kv.me, reply.ConfigNum)
				kv.handleMigration(&reply)
				DPrintf("[%d %d] MIGRATION COMPLETE %v %v", kv.gid, kv.me, reply.ConfigNum, kv.serveShards)

			} else {

				cmd := response.Command.(Op)
				k := cmd.Key
				v := cmd.Value
				op := cmd.Opr
				clientId := cmd.ClientId
				requestId := cmd.RequestId
				shard := key2shard(k)

				// DPrintf("[%d] RESPONSE client [%d] requestid [%d] index [%d]", kv.me, clientId, requestId, index)
				kv.mu.Lock()
				
				if !kv.handleKey(k){
					kv.mu.Unlock()
					continue
				}

				kv.mu.Unlock()
				ch := kv.checkChannel(index)
				kv.mu.Lock()

				_, ok := kv.kvStore[shard]
				if !ok {
					kv.kvStore[shard] = make(map[string]string)
				}

				isDup := kv.checkDup(clientId, requestId)
				if isDup {
					cmd.Value = kv.kvStore[shard][k]
				} else {
					if op == "Get" {
						cmd.Value = kv.kvStore[shard][k]
						kv.dupTable[clientId] = requestId
					} else if op == "Append" {
						kv.kvStore[shard][k] += v
						cmd.Value = kv.kvStore[shard][k]
						kv.dupTable[clientId] = requestId
					} else if op == "Put" {
						kv.kvStore[shard][k] = v
						cmd.Value = kv.kvStore[shard][k]
						kv.dupTable[clientId] = requestId
					}
				}
				// DPrintf("[%d] APPLIED client [%d] requestid [%d] index [%d]", kv.me, clientId, requestId, index)
				kv.mu.Unlock()
				ch <- cmd
			}
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






func (kv *ShardKV) pollConfigChanges() {
	for {
		_, isLeader := kv.rf.GetState()
		
		if isLeader {
			kv.mu.Lock()
			canPullLatestConfig := true
			
			for _, status := range kv.serveShards {
				if status != "serve" {
					canPullLatestConfig = false
					break
				}
			}

			if canPullLatestConfig {
				nextConfig := kv.currentConfig.Num + 1
				newConfig := kv.mck.Query(nextConfig)
				
				if newConfig.Num == nextConfig {
						DPrintf("[%d %d] CURRENT CONFIG %d %v", kv.gid, kv.me, kv.currentConfig.Num, kv.currentConfig.Shards)
						DPrintf("[%d %d] NEW CONFIG %d %v", kv.gid, kv.me, newConfig.Num, newConfig.Shards)
						kv.rf.Start(newConfig)
				}
			}
			kv.mu.Unlock()
		}

		time.Sleep(50 * time.Millisecond)
	}
}






func (kv *ShardKV) handleConfigChange(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if newConfig.Num <= kv.currentConfig.Num {
		return
	}

	DPrintf("[%d %d] HANDLE CONFIG CHANGE %d %v", kv.gid, kv.me, newConfig.Num, newConfig.Shards)

	kv.serveShards = make(map[int]string)
	
	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if newConfig.Num == 1 {
				kv.serveShards[shard] = "serve"
			} else {
				kv.serveShards[shard] = "wait"
			}
		}
	}
	
	for shard, gid := range kv.currentConfig.Shards {
		_, present := kv.serveShards[shard]
		if gid == kv.gid && present {
			kv.serveShards[shard] = "serve"
		}
	}

	DPrintf("[%d %d] SERVE SHARDS %v", kv.gid, kv.me, kv.serveShards)

	kv.lastConfig = kv.currentConfig
	kv.currentConfig = newConfig

}




func (kv *ShardKV) handleMigration(reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply.ConfigNum != kv.currentConfig.Num-1 {
		return
	}

	DPrintf("[%d %d] UPDATING KV CONFIG [%d]", kv.gid, kv.me, reply.ConfigNum)

	for shard, kvData := range reply.Data {
		for k, v := range kvData {
			if _, ok := kv.kvStore[shard]; !ok {
				kv.kvStore[shard] = make(map[string]string)
			}
			kv.kvStore[shard][k] = v
		}
		kv.serveShards[shard] = "serve"
	}

	for k, v := range reply.Dup {
		if _, ok := kv.dupTable[k]; !ok || kv.dupTable[k] < v {
			kv.dupTable[k] = v
		}
	}
	
	DPrintf("[%d %d] UPDATED SERVE SHARDS %v", kv.gid, kv.me, kv.serveShards)
}




func (kv *ShardKV) pullData() {

	for {

		_, isLeader := kv.rf.GetState()

		if isLeader {
			kv.mu.Lock()
			requestData := make(map[int][]int)
			for shard, status := range kv.serveShards {
				if status == "wait" {
					gid := kv.lastConfig.Shards[shard]
					if _, ok := requestData[gid]; !ok {
						requestData[gid] = make([]int, 0)
					}
					requestData[gid] = append(requestData[gid], shard)
				}
			}

			var wg sync.WaitGroup
			for gid, shards := range requestData {
				args := MigrateArgs{
					Shards: shards,
					ConfigNum: kv.currentConfig.Num-1,
				}
				wg.Add(1)
				go kv.sendMigrateRPC(&wg, &args, kv.lastConfig.Groups[gid])
			}

			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(20 * time.Millisecond)
	}
	
}



func (kv *ShardKV) sendMigrateRPC(wg *sync.WaitGroup, args *MigrateArgs, servers []string) {
	defer wg.Done()
	for _, server := range servers {
		srv := kv.make_end(server)
		reply := MigrateReply{}
		ok := srv.Call("ShardKV.Migrate", args, &reply)
		if ok && reply.Err == OK {
			kv.rf.Start(reply)
		}
	}
}


func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.currentConfig.Num {

		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("[%d %d] MIGRATE shards %d %v", kv.gid, kv.me, args.ConfigNum, args.Shards)

	reply.Data = make(map[int]map[string]string)
	reply.Dup = make(map[int64]int)
	reply.ConfigNum = args.ConfigNum

	for _, shard := range args.Shards {
		reply.Data[shard] = make(map[string]string)
		for k, v := range kv.kvStore[shard] {
			reply.Data[shard][k] = v
		}
	}

	for k, v := range kv.dupTable {
		reply.Dup[k] = v
	}

	reply.Err = OK
}



// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("[%d %d] Starting server", gid, me)
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[int]map[string]string)
	kv.responseCh = make(map[int]chan Op)
	kv.dupTable = make(map[int64]int)
	kv.serveShards = make(map[int]string)
	kv.lastApplied = 0
	kv.currentConfig = kv.mck.Query(0)
	kv.lastConfig = kv.mck.Query(0)

	kv.restoreState(persister.ReadSnapshot())

	go kv.receiveUpdates()

	go kv.pollConfigChanges()

	go kv.pullData()

	return kv
}
