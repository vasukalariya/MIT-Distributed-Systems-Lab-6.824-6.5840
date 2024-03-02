package shardctrler

import (
	"log"
	"sync"
	"time"
	"slices"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	dupTable    map[int64]int
	responseCh  map[int]chan Op
	lastApplied int

	currentConfig Config

	configs []Config // indexed by config num
}

func (kv *ShardCtrler) sameOps(a Op, b Op) bool {
	return a.ClientId == b.ClientId &&
		a.RequestId == b.RequestId &&
		a.Opr == b.Opr
}

type Op struct {
	// Your data here.
	Opr       string
	ClientId  int64
	RequestId int
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

func (sc *ShardCtrler) checkDup(clientId int64, requestId int) bool {
	reqId, ok := sc.dupTable[clientId]
	if !ok || reqId < requestId {
		return false
	}
	return true
}

func (sc *ShardCtrler) checkChannel(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, ok := sc.responseCh[index]
	if !ok {
		sc.responseCh[index] = make(chan Op, 1)
	}
	return sc.responseCh[index]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	isDup := sc.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		Opr:       "Join",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Servers:   args.Servers,
	}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case op := <-ch:
		if sc.sameOps(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = OK
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	isDup := sc.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		Opr:       "Leave",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		GIDs:      args.GIDs,
	}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case op := <-ch:
		if sc.sameOps(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = OK
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	isDup := sc.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		Opr:       "Move",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case op := <-ch:
		if sc.sameOps(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = OK
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	isDup := sc.checkDup(args.ClientId, args.RequestId)
	if isDup {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		Opr:       "Query",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num:       args.Num,
	}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = OK
		return
	}

	ch := sc.checkChannel(index)

	select {
	case op := <-ch:
		if sc.sameOps(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
			sc.mu.Lock()
			reply.Config = sc.configs[op.Num]
			sc.mu.Unlock()
		} else {
			reply.WrongLeader = true
			reply.Err = OK
		}
	case <-time.After(1000 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = OK
	}
}

func (sc *ShardCtrler) receiveUpdates() {

	for response := range sc.applyCh {
		if response.CommandValid {
			cmd := response.Command.(Op)
			index := response.CommandIndex
			op := cmd.Opr
			clientId := cmd.ClientId
			requestId := cmd.RequestId

			sc.mu.Lock()
			if sc.lastApplied >= index {
				sc.mu.Unlock()
				continue
			}

			sc.mu.Unlock()
			ch := sc.checkChannel(index)
			sc.mu.Lock()

			isDup := sc.checkDup(clientId, requestId)
			if !isDup {
				if op == "Join" {
					sc.doJoin(cmd.Servers)
				} else if op == "Leave" {
					sc.doLeave(cmd.GIDs)
				} else if op == "Move" {
					sc.doMove(cmd.Shard, cmd.GID)
				} else if op == "Query" {
					if cmd.Num == -1 || cmd.Num >= len(sc.configs) {
						cmd.Num = len(sc.configs) - 1
					}
				}
				sc.dupTable[clientId] = requestId
			}
			sc.lastApplied = index
			sc.mu.Unlock()
			ch <- cmd
		}
	}

}


func (sc *ShardCtrler) balanceLoad() {
	activeGids := len(sc.currentConfig.Groups)
	
	// Create a sorted list of gids because map iteration order is not deterministic
	keys := make([]int, 0)
	for gid, _ := range sc.currentConfig.Groups {
		keys = append(keys, gid)
	}

	slices.Sort(keys)

	if activeGids == 0 {
		return
	}

	mappings := make(map[int][]int)
	allot := make([]int, 0)

	avg := NShards / activeGids
	max := avg+1
	remain := NShards % activeGids

	// Create mappings of gid to shards
	for i, gid := range sc.currentConfig.Shards {
		if gid == 0 {
			allot = append(allot, i)
		} else {
			_, ok := mappings[gid]
			if !ok {
				mappings[gid] = make([]int, 0)
			}
			mappings[gid] = append(mappings[gid], i)
		} 
	}

	// Remove extra shards from each gid
	for _, gid := range keys {
		cutoff := avg
		if remain > 0 {
			cutoff = max
		}
		if len(mappings[gid]) > cutoff {
			extra := mappings[gid][cutoff:]
			allot = append(allot, extra...)
			mappings[gid] = mappings[gid][:cutoff]
			if cutoff == max {
				remain--
			}
		}
	}

	// Assigned balanced gids
	for _, gid := range keys {
		shards := mappings[gid]
		diff := avg - len(shards)
		if diff >= 0 && len(allot) > 0 {
			if diff > len(allot) {
				diff = len(allot)
			}
			mappings[gid] = append(mappings[gid], allot[:diff]...)
			allot = allot[diff:]
		}
	}


	// Assign remaining shards
	for _, gid := range keys {
		if len(allot) == 0 {
			break
		}
		shards := mappings[gid]
		diff := avg - len(shards)
		if diff == 0 {
			mappings[gid] = append(mappings[gid], allot[0])
			allot = allot[1:]
		}
	}
	
	// Update the shard mappings
	for _, gid := range keys {
		for _, shard := range mappings[gid] {
			sc.currentConfig.Shards[shard] = gid
		}
	}

}



func (sc *ShardCtrler) saveConfig() {

	sc.currentConfig.Num++
	config := Config{
		Num:    sc.currentConfig.Num,
		Shards: sc.currentConfig.Shards,
	}

	config.Groups = make(map[int][]string)

	for key, val := range sc.currentConfig.Groups {
		config.Groups[key] = val
	}

	sc.configs = append(sc.configs, config)
}



func (sc *ShardCtrler) doJoin(servers map[int][]string) {

	for key, val := range servers {
		_, ok := sc.currentConfig.Groups[key]

		if !ok {
			sc.currentConfig.Groups[key] = make([]string, 0)
		}

		sc.currentConfig.Groups[key] = append(sc.currentConfig.Groups[key], val...)
	}

	sc.balanceLoad()
	sc.saveConfig()
}


func (sc *ShardCtrler) doLeave(gids []int) {

	for _, gid := range gids {
		delete(sc.currentConfig.Groups, gid)
	}

	for i, gid := range sc.currentConfig.Shards {
		for _, val := range gids {
			if gid == val {
				sc.currentConfig.Shards[i] = 0
			}
		}
	}

	sc.balanceLoad()
	sc.saveConfig()
}



func (sc *ShardCtrler) doMove(shard int, gid int) {

	sc.currentConfig.Shards[shard] = gid
	sc.saveConfig()
	
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dupTable = make(map[int64]int)
	sc.responseCh = make(map[int]chan Op)
	sc.lastApplied = 0
	sc.currentConfig = sc.configs[len(sc.configs)-1]

	go sc.receiveUpdates()

	return sc
}
