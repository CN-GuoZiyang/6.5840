package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const (
	RequestInterval = 10 // ms
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

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:        num,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d query %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := QueryReply{}
		if ck.servers[leaderId].Call("ShardCtrler.Query", &args, &reply) {
			if reply.Err == OK {
				DPrintf("client %d query %+v success res %+v", ck.clientID, args, reply.Config)
				return reply.Config
			}
		}
		leaderId = ck.changeLeader()
		time.Sleep(RequestInterval * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		Servers:    servers,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d join %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := JoinReply{}
		if ck.servers[leaderId].Call("ShardCtrler.Join", &args, &reply) {
			if reply.Err == OK {
				DPrintf("client %d join %+v success", ck.clientID, args)
				return
			}
		}
		leaderId = ck.changeLeader()
		time.Sleep(RequestInterval * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs:       gids,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d leave %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := LeaveReply{}
		if ck.servers[leaderId].Call("ShardCtrler.Leave", &args, &reply) {
			if reply.Err == OK {
				DPrintf("client %d leave %+v success", ck.clientID, args)
				return
			}
		}
		leaderId = ck.changeLeader()
		time.Sleep(RequestInterval * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard:      shard,
		GID:        gid,
		ClientID:   ck.clientID,
		SequenceID: atomic.AddInt64(&ck.sequenceID, 1),
	}

	DPrintf("client %d move %+v", ck.clientID, args)
	leaderId := ck.getCurrentLeader()
	for {
		reply := MoveReply{}
		if ck.servers[leaderId].Call("ShardCtrler.Move", &args, &reply) {
			if reply.Err == OK {
				DPrintf("client %d move %+v success", ck.clientID, args)
				return
			}
		}
		leaderId = ck.changeLeader()
		time.Sleep(RequestInterval * time.Millisecond)
	}
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
