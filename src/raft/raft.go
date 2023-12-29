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
	//	"bytes"
	// "log"
	// "math"
	// "log"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	commitIndex int
	lastApplied int

	nextIndex[] int
	matchIndex[] int

	currentLeader int
	state string

	lastPing time.Time

	commitChannel chan ApplyMsg

	termIndex int
}


type LogEntry struct {
	
	Term int
	Command interface{}
	Index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	
	term = rf.currentTerm
	isleader = (rf.currentLeader == rf.me)
	// DPrintf("Get state called on [%d] having term [%d] and it is [%t] leader", rf.me, term, isleader)
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
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("vote requested by cand [%d] to [%d]", args.CandidateId, rf.me)	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || rf.currentTerm < args.Term {
		if args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm == rf.log[args.LastLogIndex].Term {
			DPrintf("vote given to cand [%d] by [%d] myll [%d] cll [%d]", args.CandidateId, rf.me, len(rf.log)-1, args.LastLogIndex)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = "follower"
			rf.lastPing = time.Now()
			return
		}
	}
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	

}

type AppendEntriesArgs struct {

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] LogEntry
	LeaderCommit int

}

type AppendEntriesReply struct {

	Term int
	Success bool
	Index int

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[%d] recevied append entries sent by [%d] at %v", rf.me, args.LeaderId, time.Now().UnixMilli())
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("leader term is [%d] which is less than [%d]", args.Term, rf.currentTerm)
		rf.state = "candidate"
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} 
	
	rf.currentLeader = args.LeaderId
	rf.currentTerm = args.Term
	rf.state = "follower"
	rf.lastPing = time.Now()
	rf.votedFor = -1

	lastIndex := len(rf.log)-1

	if lastIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Logs not in sync cause lterm [%d] aterm [%d] ", rf.log[args.PrevLogIndex].Term, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	start := false

	for _, newLog := range args.Entries {
		if lastIndex >= newLog.Index {
			if start {
				rf.log[newLog.Index] = newLog
			} else {
				if rf.log[newLog.Index].Term == newLog.Term {
					continue
				} else {
					start = true
				}	
			}		
		} else {
			DPrintf("[%d] Adding entry at index [%d] while leader commit [%d]", rf.me, len(rf.log), args.LeaderCommit)
			rf.log = append(rf.log, newLog)
		}
	}


	if len(args.Entries) > 0 {
		lastEntryIndex := args.Entries[len(args.Entries)-1].Index
		// delete the extra entries in the follower log 
		// to make it consistent with the leader log
		if lastIndex > lastEntryIndex {
			DPrintf("Deleting extra logs")
			rf.log = rf.log[:lastEntryIndex+1]
		}
	}

	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)


	for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
		DPrintf("[%d] is commiting entry at [%d]", rf.me, i)
		rf.commitChannel<-ApplyMsg{
			true,
			rf.log[i].Command,
			i,
			true,
			nil,
			-1,
			-1,
		}
		rf.lastApplied++
	}
	

	reply.Term = rf.currentTerm
	reply.Success = true
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	if rf.state == "leader" {
		isLeader = true
	
		prevIndex := len(rf.log)-1
		newLog := LogEntry{rf.currentTerm, command, prevIndex+1}
		rf.log = append(rf.log, newLog)
		index = newLog.Index
		term = newLog.Term
		DPrintf("[%d] new entry during term [%v] having index in log [%d]", rf.me, rf.currentTerm, newLog.Index)
		
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

func (rf *Raft) manageLeader() {
	// send entries from nextIndex to all followers
	// DPrintf("[%d] called manage leader", rf.me)

	rf.sendUpdates()
	
	//update the commitIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()

	cnt := map[int]int{}
	maxCount := 0
	maxCommit := 0

	for i,v := range rf.matchIndex {
		if i == rf.me {
			continue
		}
		cnt[v]++
		if cnt[v] > maxCount {
			maxCount = cnt[v]
			maxCommit = v
		} else if cnt[v] == maxCount {
			maxCommit = max(v, maxCommit)
		}
	}
	DPrintf("[%d][%s] Match Index: %v with count [%d] for index [%d]",rf.me, rf.state, rf.matchIndex, maxCount, maxCommit)

	if maxCount >= (len(rf.peers)-1)/2 {
		DPrintf("[%d] Received majority for max commit [%d]", rf.me, maxCommit)
		if maxCommit > rf.commitIndex && rf.log[maxCommit].Term == rf.currentTerm {
			rf.commitIndex = maxCommit
		}
	}

	for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
		rf.commitChannel<-ApplyMsg{
			true,
			rf.log[i].Command,
			i,
			true,
			nil,
			-1,
			-1,
		}
		rf.lastApplied++
	}

}



func (rf *Raft) sendUpdates() {
	// DPrintf("[%d] sending updates with state [%s]", rf.me, rf.state)
	// send updates to peers based on nextIndex


	var mut sync.Mutex
	cv := sync.NewCond(&mut)
	count := 1
	finished := 1

	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()

		PrevLogIndex := rf.nextIndex[i]-1
		PrevLogTerm := rf.log[PrevLogIndex].Term
		newEntries := append([]LogEntry(nil), rf.log[PrevLogIndex+1:]...)
		
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: PrevLogIndex,
			PrevLogTerm: PrevLogTerm,
			Entries: newEntries,
			LeaderCommit: rf.commitIndex,
		}

		reply := AppendEntriesReply{}

		rf.mu.Unlock()

		go func (peer int, args *AppendEntriesArgs, reply *AppendEntriesReply)  {
			
			ok := rf.sendAppendEntries(peer, args, reply)


			rf.mu.Lock()
			defer rf.mu.Unlock()

			mut.Lock()
			defer mut.Unlock()

			finished++
			
			if args.Term != rf.currentTerm {
				cv.Signal()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				cv.Signal()
				return
			}

			if reply.Success {
				if args.Entries != nil {
					rf.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index+1
					rf.matchIndex[peer] =  rf.nextIndex[peer]-1;
					count++
				}
			} else {
				// DPrintf("Recevied false reply from [%d] for term [%d]", peer, reply.Term)
				if ok {
					rf.nextIndex[peer]--;
				}
			}
			cv.Broadcast()
		} (i, &args, &reply)
	}
	
	mut.Lock()
	for count <= len(rf.peers)/2 || finished < len(rf.peers) {
		cv.Wait()
	}
	mut.Unlock()

	DPrintf("[%d] Returned from update", rf.me)
}


func (rf *Raft) ticker() {
	
	for rf.killed() == false {
		rf.mu.Lock()
		// Your code here (2A)
		// Check if a leader election should be started.

		if rf.state != "leader" {
			timeDiff := time.Now().Sub(rf.lastPing).Milliseconds() + int64(rand.Intn(100))
			if timeDiff >= 500 {
				rf.mu.Unlock()
				rf.AttemptElection()
				rf.mu.Lock()
			}
		} else {
			rf.mu.Unlock()
			rf.manageLeader()
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

	}

}


func (rf *Raft) AttemptElection() {
	rf.mu.Lock()

	DPrintf("Election kicked off by [%d] for term [%d] at %v", rf.me, rf.currentTerm+1, time.Now().UnixMilli())
	rf.state = "candidate"
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.lastPing = time.Now()

	servers := len(rf.peers)
	votes := 1

	rf.mu.Unlock()

	for i := 0; i < servers; i++ {
		lastIdx := len(rf.log)-1
		lastTerm := -1
		if lastIdx >= 0 {
			lastTerm = rf.log[lastIdx].Term
		}

		if i == rf.me {
			continue
		}

		go func(server int, myself, term int, lastLogIndex int, lastLogTerm int) {
			args := RequestVoteArgs{
				Term: term, 
				CandidateId: myself, 
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}

			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
			
			rf.mu.Lock()

			if rf.state != "candidate" || rf.currentTerm != term {
				// DPrintf("Early return current Term: [%d] and reply term: [%d]", rf.currentTerm, reply.Term)
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
			}
			if votes >= (servers/2 + 1) && rf.state == "candidate" {
				DPrintf("Elected Leader: [%d]", rf.me)
				rf.state = "leader"
				rf.currentLeader = rf.me
				rf.termIndex = len(rf.log)

				for i,_ := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
			} 

			rf.mu.Unlock()
			if rf.state == "leader" {
				rf.manageLeader()
			}
			
		}(i, rf.me, rf.currentTerm, lastIdx, lastTerm)	
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
		commitChannel: applyCh,
		log: []LogEntry{LogEntry{0,nil,0}},
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	for i,_ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
