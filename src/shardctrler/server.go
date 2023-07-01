package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs     []Config           // indexed by config num
	waitMap     map[int]*OpContext // 用于唤醒等待 log id 的上下文
	sequenceMap map[int64]int64    // 客户端的 sequence ID
}

type awakeMsg struct {
	wrongLeader bool
	ignored     bool
}

const (
	OpType_Join  = "Join"
	OpType_Leave = "Leave"
	OpType_Move  = "Move"
	OpType_Query = "Query"
)

type Op struct {
	Type        string
	SequenceID  int64
	ClientID    int64
	JoinServers map[int][]string
	LeaveGids   []int
}

type OpContext struct {
	origin Op
	index  int
	term   int
	ok     chan awakeMsg
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err = OK
	op := Op{
		Type:        OpType_Join,
		ClientID:    args.ClientID,
		SequenceID:  args.SequenceID,
		JoinServers: args.Servers,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d join start %+v", sc.me, op)

	opCtx := &OpContext{
		origin: op,
		index:  index,
		term:   term,
		ok:     make(chan awakeMsg),
	}
	func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		sc.waitMap[index] = opCtx
	}()
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		if item := sc.waitMap[index]; item == opCtx {
			delete(sc.waitMap, index)
		}
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case msg := <-opCtx.ok:
		DPrintf("server %d join res %+v", sc.me, msg)
		if msg.wrongLeader {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err = OK
	op := Op{
		Type:       OpType_Leave,
		ClientID:   args.ClientID,
		SequenceID: args.SequenceID,
		LeaveGids:  args.GIDs,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server %d leave start %+v", sc.me, op)

	opCtx := &OpContext{
		origin: op,
		index:  index,
		term:   term,
		ok:     make(chan awakeMsg),
	}
	func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		sc.waitMap[index] = opCtx
	}()
	defer func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		if item := sc.waitMap[index]; item == opCtx {
			delete(sc.waitMap, index)
		}
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case msg := <-opCtx.ok:
		DPrintf("server %d leave res %+v", sc.me, msg)
		if msg.wrongLeader {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.waitMap = map[int]*OpContext{}
	sc.sequenceMap = map[int64]int64{}
	go sc.applyRoutine()

	return sc
}

func (sc *ShardCtrler) applyRoutine() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			// 提交命令
			cmd := msg.Command
			index := msg.CommandIndex
			msgTerm := msg.CommandTerm
			func() {
				sc.mu.Lock()
				defer sc.mu.Unlock()

				op := cmd.(Op)
				prevSeq, existSeq := sc.sequenceMap[op.ClientID]
				sc.sequenceMap[op.ClientID] = op.SequenceID

				opCtx, hasWait := sc.waitMap[index]
				msg := awakeMsg{}
				defer func() {
					if !hasWait {
						return
					}
					if opCtx.term != msgTerm {
						msg.wrongLeader = true
					}
					DPrintf("server %d apply res %+v", sc.me, msg)
					opCtx.ok <- msg
					close(opCtx.ok)
				}()

				if existSeq && op.SequenceID <= prevSeq {
					// seqid 过期
					msg.ignored = true
					return
				}

				DPrintf("server %d apply %+v", sc.me, op)

				// Join 操作
				if op.Type == OpType_Join {
					sc.configs = append(sc.configs, *sc.MakeJoinConfig(op.JoinServers))
					return
				}

				// Leave 操作
				if op.Type == OpType_Leave {
					sc.configs = append(sc.configs, *sc.MakeLeaveConfig(op.LeaveGids))
					return
				}
			}()
		}
	}
}

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {
	newestConfig := sc.configs[len(sc.configs)-1]
	tmpGroups := map[int][]string{}
	for gid, serverList := range newestConfig.Groups {
		// reuse old
		tmpGroups[gid] = serverList
	}
	for gid, serverList := range servers {
		tmpGroups[gid] = serverList
	}

	gids := make([]int, 0, len(tmpGroups))
	for gid := range tmpGroups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	gid2Shards := map[int][]int{}
	for shard, gid := range newestConfig.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], shard)
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.rebalanceShards(gids, gid2Shards),
		Groups: tmpGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(leaveGids []int) *Config {
	newestConfig := sc.configs[len(sc.configs)-1]
	tmpGroups := map[int][]string{}
	for gid, serverList := range newestConfig.Groups {
		// reuse old
		tmpGroups[gid] = serverList
	}
	for _, gid := range leaveGids {
		delete(tmpGroups, gid)
	}

	gids := make([]int, 0, len(tmpGroups))
	for gid := range tmpGroups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	gid2Shards := map[int][]int{}
	for shard, gid := range newestConfig.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], shard)
	}

	return &Config{
		Num:    len(sc.configs),
		Shards: sc.rebalanceShards(gids, gid2Shards),
		Groups: tmpGroups,
	}
}

// 传入 gid 保证遍历顺序稳定
func (sc *ShardCtrler) rebalanceShards(gids []int, gid2Shards map[int][]int) [NShards]int {
	var res [NShards]int
	if len(gids) == 0 {
		return res
	}
	length := len(gids)
	avg := NShards / length // 平均每个 group 需要管理的 shard 个数

	gidMap := map[int]struct{}{}
	for _, gid := range gids {
		gidMap[gid] = struct{}{}
	}

	var extraShard []int
	// 超出平均的退
	for _, gid := range sortMapIndex(gid2Shards) {
		shards := gid2Shards[gid]
		if _, in := gidMap[gid]; !in {
			// gid 下线
			extraShard = append(extraShard, shards...)
			gid2Shards[gid] = nil
			continue
		}
		if off := len(shards) - avg; off > 0 {
			extraShard = append(extraShard, shards[:off]...)
			gid2Shards[gid] = gid2Shards[gid][off:]
		}
	}

	// 不到平均的补
	for _, gid := range gids {
		shards := gid2Shards[gid]
		if off := avg - len(shards); off > 0 {
			gid2Shards[gid] = append(gid2Shards[gid], extraShard[:off]...)
			extraShard = extraShard[off:]
		}
	}

	// extra 还剩
	for _, gid := range gids {
		if len(extraShard) == 0 {
			break
		}
		gid2Shards[gid] = append(gid2Shards[gid], extraShard[0])
		extraShard = extraShard[1:]
	}

	// 转回 shards array
	for gid, shards := range gid2Shards {
		for _, shard := range shards {
			res[shard] = gid
		}
	}
	return res
}

func sortMapIndex(m map[int][]int) []int {
	res := make([]int, 0, len(m))
	for idx := range m {
		res = append(res, idx)
	}
	sort.Ints(res)
	return res
}
