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
	Servable          = iota // 归该 Server 管理且可提供服务，或不归该 Server 管理
	Pulling                  // 该分片当前配置由该 Server 管理，但还未从上个配置中拉取，不可提供服务
	WaitOtherDeleting        // 该分片当前配置由该 Server 管理且可提供服务，但上个配置管理该分片的 Server 尚未删除（Pushing），可提供服务
	Pushing                  // 该分片当前配置不由该 Server 管理，但上个配置由该 Server 管理，不可提供服务
)

// 存储 Server 管理一个 Shard 的内容
type Shard struct {
	ShardStatus ShardStatus
	Store       map[string]string // Shard 实际存储
}

func (shard *Shard) copyStore() map[string]string {
	res := map[string]string{}
	for k, v := range shard.Store {
		res[k] = v
	}
	return res
}

type awakeMsg struct {
	err     Err
	ignored bool
	value   string
	exist   bool
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
	// DPrintf("server %d get start %+v", kv.me, op)

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
		// DPrintf("server %d get res %+v", kv.me, msg)
		reply.Value = msg.value
		reply.Err = msg.err
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
	// DPrintf("server %d put append start %+v", kv.me, op)

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
		reply.Err = msg.err
	case <-timer.C:
		reply.Err = ErrWrongLeader
		// DPrintf("server %d timeout", kv.me)
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
		Shards: [shardctrler.NShards]int{},
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
	labgob.Register(&shardctrler.Config{})
	labgob.Register(&ShardOpReply{})
	labgob.Register(&ShardOpArgs{})

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
	for !kv.killed() {
		canPullLatest := true
		_, isLeader := kv.rf.GetState()
		kv.mu.RLock()
		if !isLeader {
			kv.mu.RUnlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, shard := range kv.shardMap {
			if shard.ShardStatus != Servable {
				// 只有所有 shard 的状态都稳定才可以拉最新配置
				canPullLatest = false
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		currentCfgNum := kv.currentCfg.Num
		kv.mu.RUnlock()
		if canPullLatest {
			nextCfg := kv.mck.Query(currentCfgNum + 1)
			if nextCfg.Num == currentCfgNum+1 {
				DPrintf("gid %d: get latest cfg start %+v", kv.gid, nextCfg)
				_, _, _ = kv.rf.Start(Op{
					Type: OpTypeConfig,
					Data: &nextCfg,
				})
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShard() {
	for !kv.killed() {
		kv.mu.RLock()
		gid2ShardIDs := map[int][]int{}
		for shardID, shard := range kv.shardMap {
			if shard.ShardStatus == Pulling {
				gid := kv.lastCfg.Shards[shardID]
				gid2ShardIDs[gid] = append(gid2ShardIDs[gid], shardID)
			}
		}
		var wg sync.WaitGroup
		for gid, shardIDs := range gid2ShardIDs {
			wg.Add(1)
			go func(servers []string, configNum int, shardIDs []int) {
				defer wg.Done()
				req := ShardOpArgs{
					ShardIDs:  shardIDs,
					ConfigNum: configNum,
				}
				DPrintf("gid %d: get shards %+v", kv.gid, shardIDs)
				for _, server := range servers {
					var resp ShardOpReply
					srv := kv.make_end(server)
					if srv.Call("ShardKV.GetShards", &req, &resp) && resp.Err == OK {
						_, _, _ = kv.rf.Start(Op{
							Type: OpTypeInsertShard,
							Data: &resp,
						})
					}
				}
			}(kv.lastCfg.Groups[gid], kv.currentCfg.Num, shardIDs)
		}
		kv.mu.RUnlock()
		wg.Wait()
		time.Sleep(80 * time.Millisecond)
	}
}

func (kv *ShardKV) GetShards(req *ShardOpArgs, resp *ShardOpReply) {
	resp.Err = OK
	if _, isLeader := kv.rf.GetState(); !isLeader {
		resp.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.currentCfg.Num < req.ConfigNum {
		resp.Err = ErrCfgNotReady
		return
	}
	resp.Store = map[int]map[string]string{}
	for _, shardID := range req.ShardIDs {
		resp.Store[shardID] = kv.shardMap[shardID].copyStore()
	}
	resp.SequenceMap = map[int64]int64{}
	for clientID, sequenceID := range kv.sequenceMap {
		resp.SequenceMap[clientID] = sequenceID
	}
	resp.ConfigNum = req.ConfigNum
}

func (kv *ShardKV) deleteShard() {
	for !kv.killed() {
		kv.mu.RLock()
		gid2ShardIDs := map[int][]int{}
		for shardID, shard := range kv.shardMap {
			if shard.ShardStatus == WaitOtherDeleting {
				gid := kv.lastCfg.Shards[shardID]
				gid2ShardIDs[gid] = append(gid2ShardIDs[gid], shardID)
			}
		}
		var wg sync.WaitGroup
		for gid, shardIDs := range gid2ShardIDs {
			wg.Add(1)
			go func(servers []string, configNum int, shardIDs []int) {
				defer wg.Done()
				req := ShardOpArgs{
					ShardIDs:  shardIDs,
					ConfigNum: configNum,
				}
				for _, server := range servers {
					var resp ShardOpReply
					srv := kv.make_end(server)
					if srv.Call("ShardKV.DeleteShards", &req, &resp) && resp.Err == OK {
						DPrintf("gid %v: delete others shards %+v", kv.gid, shardIDs)
						_, _, _ = kv.rf.Start(Op{
							Type: OpTypeDeleteShard,
							Data: &req,
						})
					}
				}
			}(kv.lastCfg.Groups[gid], kv.currentCfg.Num, shardIDs)
		}
		kv.mu.RUnlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) DeleteShards(req *ShardOpArgs, resp *ShardOpReply) {
	resp.Err = OK
	if _, isLeader := kv.rf.GetState(); !isLeader {
		resp.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	if kv.currentCfg.Num > req.ConfigNum {
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	DPrintf("gid %v: delete shards %+v start", kv.gid, req.ShardIDs)
	op := Op{
		Type: OpTypeDeleteShard,
		Data: req,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		resp.Err = ErrWrongLeader
		return
	}
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
		resp.Err = msg.err
	case <-timer.C:
		resp.Err = ErrWrongLeader
		// DPrintf("server %d timeout", kv.me)
	}
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
				case OpTypeInsertShard:
					aMsg = kv.applyInsertShard(op)
				case OpTypeDeleteShard:
					aMsg = kv.applyDeleteShard(op)
				}

				opCtx, hasWait := kv.waitMap[index]
				if !hasWait {
					return
				}
				if opCtx.term != msgTerm {
					aMsg.err = ErrWrongLeader
				}
				// DPrintf("server %d apply res %+v", kv.me, aMsg)
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
	return kv.currentCfg.Shards[shard] == kv.gid && (kv.shardMap[shard].ShardStatus == Servable || kv.shardMap[shard].ShardStatus == WaitOtherDeleting)
}

func (kv *ShardKV) applyCommand(rawOp Op) *awakeMsg {
	op := rawOp.Data.(CommandRequest)
	shard := key2shard(op.Key)
	if !kv.canServe(shard) {
		return &awakeMsg{err: ErrWrongGroup}
	}

	aMsg := &awakeMsg{err: OK}
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

	// DPrintf("server %d apply %+v", kv.me, op)

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
	res := &awakeMsg{err: OK}
	cfg := rawOp.Data.(*shardctrler.Config)
	if cfg == nil {
		DPrintf("gid %d: get latest cfg failed", kv.gid)
		return res
	}
	if cfg.Num != kv.currentCfg.Num+1 {
		DPrintf("gid %d: get latest cfg failed - %v vs %v", kv.gid, cfg.Num, kv.currentCfg.Num)
		return res
	}
	kv.updateShardStatusByCfg(cfg, &kv.currentCfg)
	DPrintf("gid %d: get latest cfg success %+v", kv.gid, cfg)
	kv.lastCfg = kv.currentCfg
	kv.currentCfg = *cfg
	return res
}

func (kv *ShardKV) applyInsertShard(rawOp Op) *awakeMsg {
	res := &awakeMsg{err: OK}
	reply := rawOp.Data.(*ShardOpReply)
	if reply.ConfigNum != kv.currentCfg.Num {
		return res
	}
	DPrintf("gid %v: get shards success", kv.gid)
	for shardID, store := range reply.Store {
		shard := kv.shardMap[shardID]
		for k, v := range store {
			shard.Store[k] = v
		}
		shard.ShardStatus = WaitOtherDeleting
	}
	for clientID, seqID := range reply.SequenceMap {
		if currentSeqID, ok := kv.sequenceMap[clientID]; !ok ||
			currentSeqID < seqID {
			kv.sequenceMap[clientID] = seqID
		}
	}
	return res
}

func (kv *ShardKV) applyDeleteShard(rawOp Op) *awakeMsg {
	res := &awakeMsg{err: OK}
	req := rawOp.Data.(*ShardOpArgs)
	if req.ConfigNum != kv.currentCfg.Num {
		return res
	}
	for _, shardID := range req.ShardIDs {
		shard := kv.shardMap[shardID]
		if shard.ShardStatus == WaitOtherDeleting {
			// DPrintf("gid %v: delete other shard %v success", kv.gid, shardID)
			shard.ShardStatus = Servable
		} else if shard.ShardStatus == Pushing {
			// DPrintf("gid %v: delete my shard %v success", kv.gid, shardID)
			shard.ShardStatus = Servable
			shard.Store = map[string]string{}
		}
	}
	return res
}

func (kv *ShardKV) updateShardStatusByCfg(cfg, lastCfg *shardctrler.Config) {
	// 根据新配置遍历全部 Shard 并更新状态
	for shardID, shard := range kv.shardMap {
		if cfg.Shards[shardID] == kv.gid && lastCfg.Num != 0 && lastCfg.Shards[shardID] != kv.gid {
			// DPrintf("gid %v: need pull %v", kv.gid, shardID)
			shard.ShardStatus = Pulling
		}
		if cfg.Shards[shardID] != kv.gid && lastCfg.Num != 0 && lastCfg.Shards[shardID] == kv.gid {
			// DPrintf("gid %v: need push %v", kv.gid, shardID)
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
