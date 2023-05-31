package raft

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
}

// 发送追加请求的协程与主协程的通信消息（发送端内部消息）
type AppendEntriesResMsg struct {
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
		resp: &reply,
	}
}

// 主协程处理追加请求返回结果
func (rf *Raft) handleAppendEntriesRes(msg AppendEntriesResMsg) {
	resp := msg.resp
	rf.rpcTermCheck(resp.Term)
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
	reply := AppendEntriesReply{}
	defer func() {
		msg.ok <- reply
	}()
	resetTimer(rf.electionTimer, RandomizedElectionTimeout())
	rf.rpcTermCheck(msg.req.Term)
	if rf.Status != Follower {
		return
	}
	if rf.CurrentTerm > msg.req.Term {
		return
	}
	prevLog := rf.getLog(msg.req.PrevLogIndex)
	if prevLog == nil {
		prevLog = &LogEntry{}
	}
	if prevLog.Term != msg.req.PrevLogTerm {
		return
	}
	ci := 0
	for i, e := range msg.req.Entries {
		for ci < len(rf.Logs) && ci < e.Index {
			// 寻找到日志追加点
			ci++
		}
		if ci >= len(rf.Logs) {
			// 接在后面即可
			rf.Logs = append(rf.Logs, msg.req.Entries[i:]...)
			break
		} else if rf.Logs[ci].Term != e.Term {
			// 截断点位任期不同，直接覆盖后续 log
			rf.Logs = append(rf.Logs[:ci], msg.req.Entries[i:]...)
			break
		}
	}
	reply.Success = true
	// 提交收到的日志
	if msg.req.LeaderCommit >= rf.CommitIndex {
		// LeaderCommit 大于自身 Commit，说明传输了可提交 Log
		rf.commitLog(0, rf.getLatestLog().Index)
	}
}
