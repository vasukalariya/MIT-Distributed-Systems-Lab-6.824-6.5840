package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log[] LogEntry
	voteCount int

	commitIndex int
	lastApplied int

	nextIndex[] int
	matchIndex[] int

	currentLeader int
	state string

	lastPing time.Time

	applyCh chan ApplyMsg
	applyCond *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm int
}


type LogEntry struct {
	
	Term int
	Command interface{}
	Index int
}


func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getPhysicalIndex(index int) int {
	return index - rf.lastIncludedIndex
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	
	term = rf.currentTerm
	isleader = (rf.state == "leader")
	// DPrintf("Get state called on [%d] having term [%d] and it is [%t] leader",heartbeatCh rf.me, term, isleader)
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)


	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&log) != nil ||
	   d.Decode(&currentTerm) != nil || 
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil {
	  DPrintf("Error in readpersist!")
	} else {
	  rf.log = log
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.lastIncludedIndex = lastIncludedIndex
	  rf.lastIncludedTerm = lastIncludedTerm
	  rf.lastApplied = lastIncludedIndex
	  rf.commitIndex = lastIncludedIndex
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("[%d] Snapshot called at index [%d]", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	compactLog := make([]LogEntry, len(rf.log) - rf.getPhysicalIndex(index))
	copy(compactLog, rf.log[index - rf.lastIncludedIndex:])
	rf.log = compactLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[0].Term
	
	rf.persistSnapshot(snapshot)

	DPrintf("[%d] Logs updated [%d - %d] size [%d]", rf.me, rf.log[0].Index, rf.getLastIndex(), len(rf.log))
	DPrintf("--------------------[%d] Raft size [%d]--------------------", rf.me, rf.persister.RaftStateSize())

	// DPrintf("[%d] Persisted snapshot at index [%d] term [%d] [%d]", rf.me, index, rf.lastIncludedTerm, rf.log[index - rf.lastIncludedIndex].Term)
	
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}


// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] LogEntry
	LeaderCommit int

}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {

	Term int
	Success bool
	Index int

}

// InstallSnapshot RPC arguments structure
type InstallSnapshotArgs struct {

	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot []byte

}

// InstallSnapshot RPC reply structure
type InstallSnapshotReply struct {

	Term int
	
}

// Use this method while holding the lock
// Step Down to the role of follower
func (rf *Raft) stepDownToFollower(term int) {
	rf.currentTerm = term
	rf.state = "follower"
	rf.votedFor = -1
	rf.voteCount = 0
}


func (rf *Raft) resetTimer() {
	rf.lastPing = time.Now()
}


// Handle the RequestVote RPC request.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("vote requested by cand [%d] to [%d %s]", args.CandidateId, rf.me, rf.state)	


	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false


	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		isNew := false
		if args.LastLogTerm > lastTerm {
			isNew = true
		}
		if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
			isNew = true
		}
		if isNew {
			DPrintf("vote granted to cand [%d] by [%d %s]", args.CandidateId, rf.me, rf.state)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = "follower"
			rf.resetTimer()
		}
	}
	
}



// Handle the AppendEntries RPC request. 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} 
	
	rf.resetTimer()

	if args.Term > rf.currentTerm {
		DPrintf("[%d] changed from [%s] to [follower] for term [%d]", rf.me, rf.state, args.Term)
		rf.stepDownToFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	prevIndex := rf.getPhysicalIndex(args.PrevLogIndex)
	// DPrintf("[%d] last index [%d] and prev index [%d] lastIncIndex [%d]", rf.me, lastIndex, args.PrevLogIndex, rf.lastIncludedIndex)

	if lastIndex < args.PrevLogIndex || 
		(args.PrevLogIndex >= rf.lastIncludedIndex && rf.log[prevIndex].Term != args.PrevLogTerm) {
		DPrintf("Logs not in sync cause lastidx [%d] prevIdx [%d] ", lastIndex, args.PrevLogIndex)
		
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Index = rf.commitIndex+1

		return
	}


	index := args.PrevLogIndex
	for i, _ := range args.Entries {
		index++
		if index <= lastIndex {
			idx := rf.getPhysicalIndex(index)
			if index < rf.lastIncludedIndex || rf.log[idx].Term == args.Entries[i].Term {
				continue
			} else {
				rf.log = rf.log[:idx]
			}	
		} 
		rf.log = append(rf.log, args.Entries[i:]...)
		DPrintf("[%d] Logs updated [%d - %d] size [%d]", rf.me, rf.log[0].Index, rf.getLastIndex(), len(rf.log))
		DPrintf("--------------------[%d] Raft size [%d]--------------------", rf.me, rf.persister.RaftStateSize())	
		break
	}


	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		rf.applyCond.Broadcast()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}


// Handle the InstallSnapshot RPC request
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}

	rf.resetTimer()
	reply.Term = args.Term

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	rf.currentLeader = args.LeaderId

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		return
	}


	lastIndex := rf.getLastIndex()

	if args.LastIncludedIndex > lastIndex || 
		rf.log[rf.getPhysicalIndex(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
			rf.log = []LogEntry{{
				args.LastIncludedTerm,
				nil,
				args.LastIncludedIndex,
			}}
	} else {
		rf.log = rf.log[args.LastIncludedIndex - rf.lastIncludedIndex:]
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.persistSnapshot(args.Snapshot)

	// msg := ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot: args.Snapshot,
	// 	SnapshotTerm: rf.lastIncludedTerm,
	// 	SnapshotIndex: rf.lastIncludedIndex,
	// }

	DPrintf("[%d] installing snapshot from [%d] [%v] lastappindex [%d] lastIncIdx [%d] size [%d]", rf.me, rf.currentLeader, rf.log, rf.lastApplied, rf.lastIncludedIndex, len(rf.log))
	DPrintf("--------------------[%d] Raft size [%d]--------------------", rf.me, rf.persister.RaftStateSize())

	// rf.applyCh<-msg

	// rf.lastApplied = rf.lastIncludedIndex
	// rf.commitIndex = rf.lastIncludedIndex

	rf.applyCond.Broadcast()
	return
}



// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return ok
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	servers := len(rf.peers)

	if rf.state != "candidate" || rf.currentTerm != args.Term || reply.Term < rf.currentTerm {
		return ok
	}

	rf.resetTimer()

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
	}

	if reply.VoteGranted {
		rf.voteCount++
	}

	if rf.voteCount >= (servers/2) {
		DPrintf("Elected Leader: [%d]", rf.me)
		rf.state = "leader"
		rf.lastPing = time.Now()
		rf.currentLeader = rf.me

		for i,_ := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastIndex()+1
			rf.matchIndex[i] = 0
		}

		rf.broadcastUpdates()
	} 

	return ok
}




// Send the AppendEntries request to peer and handle the reply.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	
	// manage reply from the append entries request
	if rf.state != "leader" || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] changed from [%s] to [follower] for term [%d] was prevlead", rf.me, rf.state, args.Term)
		rf.stepDownToFollower(reply.Term)
		rf.resetTimer()
		return ok
	}

	if reply.Success {
		if args.Entries != nil {
			rf.nextIndex[server] = max(rf.nextIndex[server], args.Entries[len(args.Entries)-1].Index+1)
			rf.matchIndex[server] =  max(rf.matchIndex[server], rf.nextIndex[server]-1);
		}
		rf.updateCommitIndex()
	} else {
		DPrintf("Recevied false reply from [%d] for term [%d]", server, reply.Term)
		rf.nextIndex[server] = reply.Index;
	}

	return ok
}

func (rf *Raft) updateCommitIndex() {
	matchIndexes := make([]int, 0)

	for i,_ := range rf.peers {
		if i == rf.me {
			matchIndexes = append(matchIndexes, rf.getLastIndex())
			continue
		}
		matchIndexes = append(matchIndexes, rf.matchIndex[i])
	}

	sort.Ints(matchIndexes)
	majorityCommit := matchIndexes[len(rf.matchIndex)/2]
	// DPrintf("[%d][%s] Match Index: %v with count [%d] for index [%d]",rf.me, rf.state, rf.matchIndex, maxCount, maxCommit)

	if majorityCommit > rf.commitIndex && rf.log[rf.getPhysicalIndex(majorityCommit)].Term == rf.currentTerm {
		DPrintf("[%d] Received majority for max commit [%d] [%v]", rf.me, majorityCommit, rf.nextIndex)
		rf.commitIndex = majorityCommit
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != "leader" || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
		rf.resetTimer()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.updateCommitIndex()

	return ok
}



// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state == "leader" {
		isLeader = true
	
		newLog := LogEntry{rf.currentTerm, command, rf.getLastIndex()+1}
		rf.log = append(rf.log, newLog)
		index = newLog.Index
		term = newLog.Term
		DPrintf("[%d] new entry during term [%v] having index in log [%d] size [%d]", rf.me, rf.currentTerm, newLog.Index, len(rf.log))
		rf.broadcastUpdates()
	} 

	return index, term, isLeader
}



// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// Broadcast updates/heartbeats to all peers 
func (rf *Raft) broadcastUpdates() {
	// send updates to peers based on nextIndex

	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
	
			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm: rf.lastIncludedTerm,
				Snapshot: rf.persister.ReadSnapshot(),
			}
			
			DPrintf("[%d] Sending snapshot at [%d] to [%d]", rf.me, rf.lastIncludedIndex, i)

			reply := InstallSnapshotReply{}
			
			go rf.sendInstallSnapshot(i, &args, &reply)

			continue
		}

		// DPrintf("[%d] sending updates to [%d] from [%d] for term [%d]", rf.me, i, rf.nextIndex[i], rf.currentTerm)

		PrevLogIndex := rf.nextIndex[i]-1
		physicalPrevLogIndex := rf.getPhysicalIndex(PrevLogIndex)
		PrevLogTerm := rf.log[physicalPrevLogIndex].Term
		newEntries := append([]LogEntry(nil), rf.log[physicalPrevLogIndex+1:]...)
		
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: PrevLogIndex,
			PrevLogTerm: PrevLogTerm,
			Entries: newEntries,
			LeaderCommit: rf.commitIndex,
		}

		reply := AppendEntriesReply{}


		go rf.sendAppendEntries(i, &args, &reply)
	}

}


// Apply the commits to the state machine
func (rf *Raft) applyCommits() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {

		if rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
				msg := ApplyMsg{
					CommandValid: false,
					SnapshotValid: true,
					Snapshot: rf.persister.ReadSnapshot(),
					SnapshotTerm: rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			} else {
				rf.lastApplied++;
				msg := ApplyMsg{
					CommandValid: true,
					Command: rf.log[rf.getPhysicalIndex(rf.lastApplied)].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
		} else {
			rf.applyCond.Wait()
		}


		// if rf.lastApplied >= rf.commitIndex {
		// 	rf.applyCond.Wait()
		// }

		// DPrintf("[%d] Applying commits when lastIncIdx [%d] log [%d - %d] lastApp [%d] commitIdx [%d]", rf.me, rf.lastIncludedIndex, rf.log[0].Index, rf.log[len(rf.log)-1].Index, rf.lastApplied, rf.commitIndex)
		// var entriesToApply []ApplyMsg
		// for i := rf.lastApplied+1; i <= rf.commitIndex; i++{
		// 	msg := ApplyMsg{
		// 		CommandValid: true,
		// 		Command: rf.log[rf.getPhysicalIndex(i)].Command,
		// 		CommandIndex: i,
		// 	}
		// 	entriesToApply = append(entriesToApply, msg)
		// 	rf.lastApplied = i
		// }

		// DPrintf("[%d] Updated lastApplied to [%d]", rf.me, rf.lastApplied)
		// rf.mu.Unlock()		

		// for _, applyMsg := range entriesToApply {
		// 	rf.mu.Lock()
		// 	lastIncIndex := rf.lastIncludedIndex
		// 	if lastIncIndex >= applyMsg.CommandIndex {
		// 		rf.mu.Unlock()
		// 		continue
		// 	}
		// 	rf.mu.Unlock()
		// 	DPrintf("[%d] applying on channel [%d] during lastApp [%d]", rf.me, applyMsg.CommandIndex, rf.lastApplied)
		// 	rf.applyCh <- applyMsg
		// }


	}
	
}


// Attempt Election and request votes
func (rf *Raft) AttemptElection() {

	defer rf.persist()
	DPrintf("Election kicked off by [%d %s] for term [%d]", rf.me, rf.state, rf.currentTerm+1)
	rf.state = "candidate"
	rf.votedFor = rf.me
	rf.voteCount = 0
	rf.currentTerm++
	rf.lastPing = time.Now()

	servers := len(rf.peers)

	term := rf.currentTerm

	for i := 0; i < servers; i++ {
		if i == rf.me {
			continue
		}

		lastIdx := rf.getLastIndex()
		lastTerm := rf.getLastTerm()

		args := RequestVoteArgs{
			Term: term, 
			CandidateId: rf.me, 
			LastLogIndex: lastIdx,
			LastLogTerm: lastTerm,
		}
	
		reply := RequestVoteReply{}

		go rf.sendRequestVote(i, &args, &reply)
	}

}

// Background process
func (rf *Raft) ticker() {

	for rf.killed() == false {
		rf.mu.Lock()
		// Your code here (2A)
		// Check if a leader election should be started.
		// DPrintf("[%d] polling", rf.me)
		if rf.state != "leader" {
			timeDiff := time.Now().Sub(rf.lastPing).Milliseconds() + int64(rand.Intn(10)*20)
			if timeDiff >= 1000 {
				rf.AttemptElection()
			}
		} else {
			rf.broadcastUpdates()
		}
		
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 10)*5
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		currentTerm: 0,
		votedFor: -1,
		commitIndex: 0,
		lastApplied: 0,
		currentLeader: -1,
		state: "follower",
		lastPing: time.Now(),
		applyCh: applyCh,
		log: []LogEntry{LogEntry{0,nil,0}},
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		lastIncludedIndex: 0,
		lastIncludedTerm: -1,
	}
	for i,_ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommits()


	return rf
}