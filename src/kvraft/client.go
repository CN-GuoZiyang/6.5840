package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientID   int64 // 客户端唯一标识
	sequenceID int64 // 客户端递增的请求 ID
	leaderID   int   // 缓存该客户端认识的 leader ID

	getCurrentLeaderChan chan getCurrentLeaderMsg
	changeLeaderChan     chan changeLeaderMsg
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
	ck.clientID = nrand()
	ck.getCurrentLeaderChan = make(chan getCurrentLeaderMsg)
	ck.changeLeaderChan = make(chan changeLeaderMsg)
	go ck.leaderRoutine()
	return ck
}

func (ck *Clerk) leaderRoutine() {
	for {
		select {
		case msg := <-ck.getCurrentLeaderChan:
			ck.handleGetCurrentLeader(msg)
		case msg := <-ck.changeLeaderChan:
			ck.handleChangeLeader(msg)
		}
	}
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
	args := GetArgs{
		Key:        key,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d get %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK {
				DPrintf("client %d get %+v res %v", ck.clientID, args, reply.Value)
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				DPrintf("client %d get %+v res %v", ck.clientID, args, "")
				return ""
			}
		}
		leaderId = ck.changeLeader()
		time.Sleep(1 * time.Millisecond)
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
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d put append %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := PutAppendReply{}
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			if reply.Err == OK {
				break
			}
		}
		leaderId = ck.changeLeader()
	}
	DPrintf("client %d put append %+v success", ck.clientID, args)
}

type getCurrentLeaderMsg struct {
	ok chan int
}

func (ck *Clerk) getCurrentLeader() int {
	msg := getCurrentLeaderMsg{
		ok: make(chan int),
	}
	ck.getCurrentLeaderChan <- msg
	res := <-msg.ok
	return res
}

func (ck *Clerk) handleGetCurrentLeader(msg getCurrentLeaderMsg) {
	msg.ok <- ck.leaderID
}

type changeLeaderMsg struct {
	ok chan int
}

func (ck *Clerk) changeLeader() int {
	msg := changeLeaderMsg{
		ok: make(chan int),
	}
	ck.changeLeaderChan <- msg
	res := <-msg.ok
	return res
}

func (ck *Clerk) handleChangeLeader(msg changeLeaderMsg) {
	ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	msg.ok <- ck.leaderID
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
