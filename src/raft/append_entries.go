package raft

import (
	"sort"
)

/********* 追加请求相关数据结构 *********/
// 追加 RPC 请求
type AppendEntriesArgs struct {
	// Term Leader 的任期
	Term int
	// LeaderID Follower 可以将客户端请求重定向到 Leader
	LeaderID int
	// PrevLogIndex 新日志条目前一个日志条目的日志索引
	PrevLogIndex int
	// PrevLogTerm 前一个日志条目的任期
	PrevLogTerm int
	// Entries 需要保存的日志条目，心跳包为空
	Entries []*LogEntry
	// LeaderCommit Leader 的 CommitIndex
	LeaderCommit int
}

// 追加 RPC 响应
type AppendEntriesReply struct {
	// Term Follower 当前任期
	Term int
	// Success Follower 包含 PrevLogIndex 和 PrevLogTerm 的日志条目为 true
	Success bool
	// 如发生冲突拒绝，冲突开始时的 Log 的 Index 和 Term
	ConflictIndex int
	ConflictTerm  int
}

// 发送追加请求的协程与主协程的通信消息（发送端内部消息）
type AppendEntriesResMsg struct {
	peer int
	args AppendEntriesArgs
	resp *AppendEntriesReply
}

// 接受追加请求的协程与主协程的通信消息（接收端内部消息）
type AppendEntriesMsg struct {
	req *AppendEntriesArgs
	ok  chan AppendEntriesReply
}

/********* 追加请求发送端相关方法 *********/
// 追加请求 RPC 发送入口
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 发送追加请求的协程
func (rf *Raft) sendAppendEntriesRoutine(peer int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return
	}
	rf.appendEntriesResChan <- AppendEntriesResMsg{
		peer: peer,
		args: args,
		resp: &reply,
	}
}

// 主协程处理追加请求返回结果
func (rf *Raft) handleAppendEntriesRes(msg AppendEntriesResMsg) {
	resp := msg.resp
	// 如果和发送 rpc 时的任期不一致，则无需处理
	if rf.CurrentTerm != msg.args.Term {
		return
	}
	if !rf.rpcTermCheck(resp.Term) {
		return
	}
	if !resp.Success {
		// 同步失败，NextIndex 根据 ConflictIndex 回退
		if resp.ConflictTerm == -1 {
			// prevLogIndex 位置没有日志，整个重新同步
			DPrintf("Leader %d: Node %d's NextIndex is %d", rf.me, msg.peer, resp.ConflictIndex)
			rf.NextIndex[msg.peer] = resp.ConflictIndex
			return
		}
		conflictTermIndex := -1
		for index := msg.args.PrevLogIndex; index >= 1; index-- {
			i := rf.logIndex2ArrayIndex(index)
			if i < 0 {
				break
			}
			if rf.Logs[i].Term == resp.ConflictTerm {
				conflictTermIndex = i
				break
			}
		}
		if conflictTermIndex != -1 {
			rf.NextIndex[msg.peer] = conflictTermIndex + 1
		} else {
			rf.NextIndex[msg.peer] = resp.ConflictIndex
		}
		return
	}
	// 更新对应的 NextIndex 和 MatchIndex
	if len(msg.args.Entries) != 0 {
		rf.MatchIndex[msg.peer] = msg.args.PrevLogIndex + len(msg.args.Entries)
		rf.NextIndex[msg.peer] = rf.MatchIndex[msg.peer] + 1
	}
	// 判断是否有 Log 已经达成共识
	var matchIndexes []int
	matchIndexes = append(matchIndexes, rf.MatchIndex...)
	sort.Ints(matchIndexes)
	allAgree := matchIndexes[len(matchIndexes)/2]
	if allAgree <= rf.CommitIndex {
		return
	}
	allAgreeLog, inSnapshot := rf.getLog(allAgree)
	if inSnapshot {
		// allAgree 位于 snapshot 内
		DPrintf("Index %d reach agree!\n", allAgree)
		defer rf.broadcastHeartbeat()
		rf.commitLog(allAgree)
		return
	}
	if allAgreeLog.Term == rf.CurrentTerm {
		// 只有当前任期的日志才需要当前 Server 提交
		DPrintf("Index %d reach agree!\n", allAgree)
		defer rf.broadcastHeartbeat()
		rf.commitLog(allAgree)
		return
	}
}

/********* 追加请求接收端相关方法 *********/
// 追加请求 RPC 接收入口
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	msg := AppendEntriesMsg{
		req: args,
		ok:  make(chan AppendEntriesReply),
	}
	rf.appendEntriesChan <- msg
	resp := <-msg.ok
	*reply = resp
}

// 主协程处理追加请求
func (rf *Raft) handleAppendEntries(msg AppendEntriesMsg) {
	reply := AppendEntriesReply{Term: rf.CurrentTerm, ConflictIndex: 1, ConflictTerm: -1}
	defer func() {
		msg.ok <- reply
	}()
	resetTimer(rf.electionTimer, RandomizedElectionTimeout())
	if rf.CurrentTerm > msg.req.Term {
		return
	}
	rf.rpcTermCheck(msg.req.Term)

	prevLog, inSnapshot := rf.getLog(msg.req.PrevLogIndex)
	if prevLog == nil && !inSnapshot {
		// 本地日志中不存在前序日志，失败
		reply.ConflictIndex = rf.getLatestIndex() + 1
		return
	}
	if inSnapshot {
		// 前序日志在 snapshot 中，且不是最后一个
		if msg.req.PrevLogIndex < rf.Logs[0].Index {
			return
		}
		// 前序日志是最后一个，但是冲突
		if msg.req.PrevLogTerm != rf.Logs[0].Term {
			return
		}
	}
	if prevLog == nil {
		prevLog = rf.Logs[0]
	}
	// 前序日志不在快照中
	if !inSnapshot {
		// 前序日志处无日志
		lastedIndex := rf.getLatestIndex()
		if msg.req.PrevLogIndex > lastedIndex {
			reply.ConflictIndex = lastedIndex + 1
			return
		}
		// 前序日志处日志冲突
		if prevLog.Term != msg.req.PrevLogTerm {
			reply.ConflictTerm = prevLog.Term
			// 找到冲突 Term 首次出现的位置
			for index := rf.Logs[0].Index + 1; index <= msg.req.PrevLogIndex; index++ {
				tempLog, _ := rf.getLog(index)
				if tempLog.Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}
	for i, entry := range msg.req.Entries {
		index := entry.Index
		if index > rf.getLatestIndex() {
			// 超出现有日志长度，直接追加
			rf.Logs = append(rf.Logs, msg.req.Entries[i:]...)
			break
		} else {
			log, _ := rf.getLog(index)
			// 存在重叠，直接覆盖
			if log.Term != entry.Term {
				rf.Logs = append(rf.Logs[:rf.logIndex2ArrayIndex(index)], msg.req.Entries[i:]...)
				break
			}
		}
	}
	rf.persister.SaveOnlyState(rf.stateData())

	reply.Success = true
	// 提交收到的日志
	if msg.req.LeaderCommit > rf.CommitIndex {
		// LeaderCommit 大于自身 Commit，说明传输了可提交 Log
		newCommitIndex := IfElseInt(rf.getLatestIndex() < msg.req.LeaderCommit, rf.getLatestIndex(), msg.req.LeaderCommit)
		DPrintf("node %d forced commit to %d\n", rf.me, newCommitIndex)
		rf.commitLog(newCommitIndex)
	}
}
