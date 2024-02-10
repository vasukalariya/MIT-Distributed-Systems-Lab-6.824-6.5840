package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	ClientId int64
	RequestId int
	// "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	RequestId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// type DupTableEntry struct {
// 	RequestId int64
// 	Key string
// 	Value string
// }