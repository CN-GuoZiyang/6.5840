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

type OpType int

const (
	OpTypeCommand = iota
	OpTypeConfig
	OpTypeInsertShard
	OpTypeDeleteShard
)

type Op struct {
	Type OpType
	Data interface{}
}

type OpContext struct {
	origin Op
	index  int
	term   int
	ok     chan awakeMsg
}

type CommandRequest struct {
	Type       string
	Key        string
	Value      string
	SequenceID int64
	ClientID   int64
}

type ShardStatus int

const (
	Servable          = iota // 归该 Server 管理且可提供服务
	Pulling                  // 该分片当前配置由该 Server 管理，但还未从上个配置中拉取，不可提供服务
	WaitOtherDeleting        // 该分片当前配置由该 Server 管理且可提供服务，但上个配置管理该分片的 Server 尚未删除（Pushing），可提供服务
	Pushing                  // 该分片当前配置不由该 Server 管理，但上个配置由该 Server 管理，不可提供服务
	Empty                    // 不归该 Server 管理，且已清空
)

// 存储 Server 管理一个 Shard 的内容
type Shard struct {
	ShardStatus ShardStatus
	Store       map[string]string // Shard 实际存储
}

type awakeMsg struct {
	wrongLeader bool
	wrongGroup  bool
	ignored     bool
	value       string
	exist       bool
}

type ShardKV struct {
	mu           *sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32
	persister    *raft.Persister

	mck *shardctrler.Clerk

	currentCfg shardctrler.Config
	lastCfg    shardctrler.Config // 迁移中时上一份配置

	shardMap         map[int]*Shard
	waitMap          map[int]*OpContext // 用于唤醒等待 log id 的上下文
	sequenceMap      map[int64]int64    // 客户端的 sequence ID
	lastAppliedIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err = OK
	op := Op{
		Type: OpTypeCommand,
		Data: CommandRequest{
			Type:       "Get",
			Key:        args.Key,
			ClientID:   args.ClientID,
			SequenceID: args.SequenceID,
		},
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
		go func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if item := kv.waitMap[index]; item == opCtx {
				delete(kv.waitMap, index)
			}
		}()
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
		Type: OpTypeCommand,
		Data: CommandRequest{
			Type:       args.Op,
			Key:        args.Key,
			Value:      args.Value,
			ClientID:   args.ClientID,
			SequenceID: args.SequenceID,
		},
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
		go func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if item := kv.waitMap[index]; item == opCtx {
				delete(kv.waitMap, index)
			}
		}()
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

func defaultConfig(gid int) shardctrler.Config {
	return shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{gid, gid, gid, gid, gid, gid, gid, gid, gid, gid},
		Groups: map[int][]string{},
	}
}

func defaultShardMap() map[int]*Shard {
	res := map[int]*Shard{}
	for i := 0; i < shardctrler.NShards; i++ {
		res[i] = &Shard{ShardStatus: Servable, Store: map[string]string{}}
	}
	return res
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
	labgob.Register(CommandRequest{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		mu:               new(sync.RWMutex),
		me:               me,
		rf:               raft.Make(servers, me, persister, applyCh),
		applyCh:          applyCh,
		make_end:         make_end,
		gid:              gid,
		ctrlers:          ctrlers,
		maxraftstate:     maxraftstate,
		dead:             0,
		persister:        persister,
		mck:              shardctrler.MakeClerk(ctrlers),
		currentCfg:       defaultConfig(gid),
		lastCfg:          defaultConfig(gid),
		shardMap:         defaultShardMap(),
		waitMap:          map[int]*OpContext{}, // 用于唤醒等待 log id 的上下文
		sequenceMap:      map[int64]int64{},    // 客户端的 sequence ID
		lastAppliedIndex: 0,
	}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) != 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.shardMap)
		d.Decode(&kv.sequenceMap)
		d.Decode(&kv.currentCfg)
		d.Decode(&kv.lastCfg)
	}

	go kv.applyRoutine()
	if maxraftstate != -1 {
		go kv.snapshotRoutine()
	}
	go kv.pullLatestCfg()
	go kv.pullShard()
	go kv.deleteShard()

	return kv
}

// 定时更新配置
func (kv *ShardKV) pullLatestCfg() {
	canPullLatest := true
	for !kv.killed() {
		kv.mu.RLock()
		for _, shard := range kv.shardMap {
			if shard.ShardStatus != Servable && shard.ShardStatus != Empty {
				// 只有所有 shard 的状态都稳定才可以拉最新配置
				canPullLatest = false
				break
			}
		}
		currentCfgNum := kv.currentCfg.Num
		kv.mu.RUnlock()
		if canPullLatest {
			nextCfg := kv.mck.Query(currentCfgNum + 1)
			if nextCfg.Num != currentCfgNum+1 {
				continue
			}
			op := Op{
				Type: OpTypeConfig,
				Data: &nextCfg,
			}

			_, _, _ = kv.rf.Start(op)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShard() {
}

func (kv *ShardKV) deleteShard() {
}

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			// 提交命令
			index := msg.CommandIndex
			msgTerm := msg.CommandTerm

			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				if index <= kv.lastAppliedIndex {
					return
				}
				kv.lastAppliedIndex = index

				var aMsg *awakeMsg
				op := msg.Command.(Op)
				switch op.Type {
				case OpTypeCommand:
					aMsg = kv.applyCommand(op)
				case OpTypeConfig:
					aMsg = kv.applyConfig(op)
				}

				opCtx, hasWait := kv.waitMap[index]
				if !hasWait {
					return
				}
				if opCtx.term != msgTerm {
					aMsg.wrongLeader = true
				}
				DPrintf("server %d apply res %+v", kv.me, aMsg)
				opCtx.ok <- *aMsg
				close(opCtx.ok)
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
				d.Decode(&kv.shardMap)
				d.Decode(&kv.sequenceMap)
				d.Decode(&kv.currentCfg)
				d.Decode(&kv.lastCfg)
			}()
		}
	}
}

func (kv *ShardKV) canServe(shard int) bool {
	return kv.shardMap[shard].ShardStatus == Servable || kv.shardMap[shard].ShardStatus == WaitOtherDeleting
}

func (kv *ShardKV) applyCommand(rawOp Op) *awakeMsg {
	op := rawOp.Data.(CommandRequest)
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return &awakeMsg{wrongGroup: true}
	}

	aMsg := &awakeMsg{}
	prevSeq, existSeq := kv.sequenceMap[op.ClientID]
	kv.sequenceMap[op.ClientID] = op.SequenceID

	// Get 操作
	if op.Type == "Get" {
		aMsg.value, aMsg.exist = kv.shardMap[shard].Store[op.Key]
		return aMsg
	}

	if existSeq && op.SequenceID <= prevSeq {
		// seqid 过期
		aMsg.ignored = true
		return aMsg
	}

	DPrintf("server %d apply %+v", kv.me, op)

	// Put 操作
	if op.Type == "Put" {
		kv.shardMap[shard].Store[op.Key] = op.Value
		return aMsg
	}

	// Append 操作
	if op.Type == "Append" {
		if val, exist := kv.shardMap[shard].Store[op.Key]; exist {
			kv.shardMap[shard].Store[op.Key] = val + op.Value
		} else {
			kv.shardMap[shard].Store[op.Key] = op.Value
		}
		return aMsg
	}

	return aMsg
}

func (kv *ShardKV) applyConfig(rawOp Op) *awakeMsg {
	res := &awakeMsg{}
	cfg := rawOp.Data.(*shardctrler.Config)
	if cfg == nil {
		return res
	}
	if cfg.Num != kv.currentCfg.Num+1 {
		return res
	}
	kv.updateShardStatusByCfg(cfg)
	kv.lastCfg = kv.currentCfg
	kv.currentCfg = *cfg
	return res
}

func (kv *ShardKV) updateShardStatusByCfg(cfg *shardctrler.Config) {
	// 根据新配置遍历全部 Shard 并更新状态
	for shardID, shard := range kv.shardMap {
		if cfg.Shards[shardID] == kv.gid && shard.ShardStatus == Empty {
			// 应当由我负责的 shard 当前不由我负责
			shard.ShardStatus = Pulling
		}
		if cfg.Shards[shardID] != kv.gid && shard.ShardStatus == Servable {
			// 不应当由我负责的 shard 当前由我负责
			shard.ShardStatus = Pushing
		}
	}
}

func (kv *ShardKV) snapshotRoutine() {
	for !kv.killed() {
		if kv.rf.ExceedSnapshotSize(kv.maxraftstate) {
			kv.mu.RLock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.shardMap)
			e.Encode(kv.sequenceMap)
			e.Encode(kv.currentCfg)
			e.Encode(kv.lastCfg)
			snapshot := w.Bytes()
			lastIncludedIndex := kv.lastAppliedIndex
			kv.mu.RUnlock()
			kv.rf.Snapshot(lastIncludedIndex, snapshot)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
