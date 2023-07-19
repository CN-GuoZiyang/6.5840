package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

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
	wrongGroup  bool
	ignored     bool
	value       string
	exist       bool
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
	dead         int32
	persister    *raft.Persister

	config shardctrler.Config
	mck    *shardctrler.Clerk

	store            map[string]string  // apply 后的 kv 存储
	waitMap          map[int]*OpContext // 用于唤醒等待 log id 的上下文
	sequenceMap      map[int64]int64    // 客户端的 sequence ID
	lastAppliedIndex int

	availableShards map[int]struct{}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		if msg.wrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
		if msg.wrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		DPrintf("server %d timeout", kv.me)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(Op{})

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

	kv.store = map[string]string{}
	kv.sequenceMap = map[int64]int64{}
	kv.waitMap = map[int]*OpContext{}

	kv.availableShards = map[int]struct{}{}

	kv.persister = persister
	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.store)
		d.Decode(&kv.sequenceMap)
	}

	go kv.pullLatestCfg()
	go kv.applyRoutine()
	if maxraftstate != -1 {
		go kv.snapshotRoutine()
	}

	return kv
}

func (kv *ShardKV) pullLatestCfg() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		func() {
			kv.mu.Lock()
			if !isLeader {
				kv.mu.Unlock()
				return
			}
			nextNum := kv.config.Num + 1
			kv.mu.Unlock()
			cfg := kv.mck.Query(nextNum)
			// 获取到了下一份配置
			if cfg.Num == nextNum {
				_, _, _ = kv.rf.Start(cfg)
			}
		}()
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			// 提交命令
			cmd := msg.Command
			op := cmd.(Op)
			index := msg.CommandIndex
			msgTerm := msg.CommandTerm

			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				kv.lastAppliedIndex = index
				prevSeq, existSeq := kv.sequenceMap[op.ClientID]
				kv.sequenceMap[op.ClientID] = op.SequenceID

				opCtx, hasWait := kv.waitMap[index]
				aMsg := awakeMsg{}
				defer func() {
					if !hasWait {
						return
					}
					if opCtx.term != msgTerm {
						aMsg.wrongLeader = true
					}
					DPrintf("server %d apply res %+v", kv.me, aMsg)
					opCtx.ok <- aMsg
					close(opCtx.ok)
				}()

				shard := key2shard(op.Key)
				if _, ok := kv.availableShards[shard]; !ok {
					aMsg.wrongGroup = true
					return
				}

				// Get 操作
				if op.Type == "Get" {
					aMsg.value, aMsg.exist = kv.store[op.Key]
					return
				}

				if existSeq && op.SequenceID <= prevSeq {
					// seqid 过期
					aMsg.ignored = true
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
		if msg.SnapshotValid {
			// 安装快照
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				if msg.SnapshotIndex <= kv.lastAppliedIndex {
					// 旧快照，忽略
					return
				}
				kv.lastAppliedIndex = msg.SnapshotIndex
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				d.Decode(&kv.store)
				d.Decode(&kv.sequenceMap)
			}()
		}
	}
}

func (kv *ShardKV) snapshotRoutine() {
	for !kv.killed() {
		if kv.rf.ExceedSnapshotSize(kv.maxraftstate) {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.store)
			e.Encode(kv.sequenceMap)
			snapshot := w.Bytes()
			lastIncludedIndex := kv.lastAppliedIndex
			kv.mu.Unlock()
			kv.rf.Snapshot(lastIncludedIndex, snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
