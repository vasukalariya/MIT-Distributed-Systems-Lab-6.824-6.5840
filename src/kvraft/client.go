package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	// "time"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	clientId int64
	requestId int
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
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.clientId = nrand()
	ck.requestId = 0
	
	return ck
}

func (ck *Clerk) getRequestId() int {
	nextReqId := ck.requestId
	ck.requestId++
	return nextReqId
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs {
		Key: key,
		ClientId: ck.clientId,
		RequestId: ck.getRequestId(), 
	}
	i := ck.lastLeader
	for {

		reply := GetReply{}
		// DPrintf("[%d] Calling Get [%s]", i, key)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err != OK {
			i = (i+1)%len(ck.servers)
			time.Sleep(time.Duration(100)*time.Microsecond)
			continue
		}

		// DPrintf("[%d %s] Returned Get [%s] [%s]", i, reply.Err, key, reply.Value)
		ck.lastLeader = i
		return reply.Value
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	
	args := PutAppendArgs {
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		RequestId: ck.getRequestId(),
	}
	i := ck.lastLeader
	for {
		reply := PutAppendReply{}
		// DPrintf("[%d] Calling (%s) [%s] [%s]", i, op, key, value)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		
		if !ok || reply.Err != OK {
			i = (i+1)%len(ck.servers)
			if i == 0 {
				time.Sleep(time.Duration(100)*time.Microsecond)
			}
			continue
		}

		ck.lastLeader = i
		// DPrintf("[%d %s] Returned (%s) [%s] [%s]", i, reply.Err, op, key, value)

		return
	}

}




func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
