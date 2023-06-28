package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
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
	Type       string // PutAppend, Get
	Key        string
	Value      string
	SequenceID int64
	ClientID   int64
}

type OpContext struct {
	origin Op
	index  int
	term   int
	ok     chan awakeMsg
}

type awakeMsg struct {
	wrongLeader bool
	ignored     bool
	value       string
	exist       bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store       map[string]string  // apply 后的 kv 存储
	waitMap     map[int]*OpContext // 用于唤醒等待 log id 的上下文
	sequenceMap map[int64]int64    // 客户端的 sequence ID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = OK
	op := Op{
		Type:       "Get",
		Key:        args.Key,
		ClientID:   args.ClientID,
		SequenceID: args.SequenceID,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d get start %+v", kv.me, op)

	opCtx := &OpContext{
		origin: op,
		index:  index,
		term:   term,
		ok:     make(chan awakeMsg),
	}
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.waitMap[index] = opCtx
	}()
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if item := kv.waitMap[index]; item == opCtx {
			delete(kv.waitMap, index)
		}
	}()

	// 2s 超时
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case msg := <-opCtx.ok:
		DPrintf("server %d get res %+v", kv.me, msg)
		reply.Value = msg.value
		if msg.wrongLeader {
			reply.Err = ErrWrongLeader
		} else if !msg.exist {
			reply.Err = ErrNoKey
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err = OK
	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		ClientID:   args.ClientID,
		SequenceID: args.SequenceID,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		// DPrintf("server %d not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d put append start %+v", kv.me, op)

	opCtx := &OpContext{
		origin: op,
		index:  index,
		term:   term,
		ok:     make(chan awakeMsg),
	}
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.waitMap[index] = opCtx
	}()
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if item := kv.waitMap[index]; item == opCtx {
			delete(kv.waitMap, index)
		}
	}()

	// 2s 超时
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case msg := <-opCtx.ok:
		if msg.wrongLeader {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		DPrintf("server %d timeout", kv.me)
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

	kv.store = map[string]string{}
	kv.sequenceMap = map[int64]int64{}
	kv.waitMap = map[int]*OpContext{}

	// You may need initialization code here.
	go kv.applyRoutine()

	return kv
}

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		cmd := msg.Command
		index := msg.CommandIndex
		msgTerm := msg.CommandTerm
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			op := cmd.(Op)
			prevSeq, existSeq := kv.sequenceMap[op.ClientID]
			kv.sequenceMap[op.ClientID] = op.SequenceID

			opCtx, hasWait := kv.waitMap[index]
			msg := awakeMsg{}
			defer func() {
				if !hasWait {
					return
				}
				if opCtx.term != msgTerm {
					msg.wrongLeader = true
				}
				DPrintf("server %d apply res %+v", kv.me, msg)
				opCtx.ok <- msg
				close(opCtx.ok)
			}()

			// Get 操作
			if op.Type == "Get" {
				msg.value, msg.exist = kv.store[op.Key]
				return
			}

			if existSeq && op.SequenceID <= prevSeq {
				// seqid 过期
				msg.ignored = true
				return
			}

			DPrintf("server %d apply %+v", kv.me, op)

			// Put 操作
			if op.Type == "Put" {
				kv.store[op.Key] = op.Value
				return
			}

			// Append 操作
			if op.Type == "Append" {
				if val, exist := kv.store[op.Key]; exist {
					kv.store[op.Key] = val + op.Value
				} else {
					kv.store[op.Key] = op.Value
				}
				return
			}
		}()
	}
}
