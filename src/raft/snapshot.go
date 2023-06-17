package raft

type outerSnapshotMsg struct {
	index    int
	snapshot []byte
	ok       chan struct{}
}

// 外部调用 Leader 存储快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	msg := outerSnapshotMsg{
		index:    index,
		snapshot: snapshot,
		ok:       make(chan struct{}),
	}
	rf.outerSnapshotChan <- msg
	<-msg.ok
}

func (rf *Raft) handleOuterSnapshot(msg outerSnapshotMsg) {
	defer func() {
		msg.ok <- struct{}{}
	}()
	snapshotLog := rf.Logs[0]
	if msg.index <= snapshotLog.Index {
		// 旧快照
		return
	}
	if msg.index > rf.CommitIndex {
		// 不允许压缩尚未提交的日志
		return
	}
	for index, log := range rf.Logs {
		if log.Index != msg.index {
			continue
		}
		snapshotLog.Index = log.Index
		snapshotLog.Term = log.Term
		tmpLogs := append(make([]*LogEntry, 0), rf.Logs[0])
		tmpLogs = append(tmpLogs, rf.Logs[index+1:]...)
		rf.Logs = tmpLogs
		break
	}
	rf.snapshot = msg.snapshot
	rf.snapshotNeedApply = false
	rf.persist()
	DPrintf("Node %d snapshot from index %d", rf.me, msg.index)
}

// Server 发送给 Follower 的同步日志的 RPC
type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type InstallSnapshotReply struct {
	Term int
}

type InstallSnapshotResMsg struct {
	peer int
	args InstallSnapshotArgs
	resp *InstallSnapshotReply
}

/********* 安装快照请求发送端相关方法 *********/
// 安装快照发送入口
func (rf *Raft) sendInstallSnapshotRoutine(peer int, snapshotLog *LogEntry) {
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: snapshotLog.Index,
		LastIncludedTerm:  snapshotLog.Term,
		SnapshotData:      rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if !ok {
		return
	}
	rf.installSnapshotResChan <- InstallSnapshotResMsg{
		peer: peer,
		args: args,
		resp: &reply,
	}
}

// 安装快照 RPC 发送 RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshotRes(msg InstallSnapshotResMsg) {
	if !rf.rpcTermCheck(msg.resp.Term) {
		return
	}
	rf.MatchIndex[msg.peer] = msg.args.LastIncludedIndex
	rf.NextIndex[msg.peer] = msg.args.LastIncludedIndex + 1
	DPrintf("Leader %d: after install snapshot, Node %d ni=%d", rf.me, msg.peer, msg.args.LastIncludedIndex+1)
	rf.judgetCommit()
}

/********* 安装快照请求接收端相关方法 *********/
// 接受追加请求的协程与主协程的通信消息（接收端内部消息）
type InstallSnapshotMsg struct {
	req *InstallSnapshotArgs
	ok  chan InstallSnapshotReply
}

// 安装快照 RPC 处理入口
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	msg := InstallSnapshotMsg{
		req: args,
		ok:  make(chan InstallSnapshotReply),
	}
	rf.installSnapshotChan <- msg
	resp := <-msg.ok
	*reply = resp
}

func (rf *Raft) handleInstallSnapshot(msg InstallSnapshotMsg) {
	DPrintf("Node %d receive installsnapshot: req.LastIncludedIndex %d vs my LastIncludedIndex %d", rf.me, msg.req.LastIncludedIndex, rf.Logs[0].Index)
	defer func() {
		msg.ok <- InstallSnapshotReply{Term: rf.CurrentTerm}
	}()

	if rf.CurrentTerm > msg.req.Term {
		// 小于当前任期，直接返回
		return
	}
	resetTimer(rf.electionTimer, RandomizedElectionTimeout())
	rf.rpcTermCheck(msg.req.Term)

	if msg.req.LastIncludedIndex <= rf.Logs[0].Index {
		// 旧快照，无需处理
		return
	}

	defer func() {
		rf.persist()
	}()
	if msg.req.LastIncludedIndex < rf.getLatestIndex() {
		// 快照外还有日志，截断
		if rf.Logs[rf.logIndex2ArrayIndex(msg.req.LastIncludedIndex)].Term != msg.req.LastIncludedTerm {
			// 截断处 term 冲突
			rf.Logs = append(make([]*LogEntry, 0), rf.Logs[0])
		} else {
			// 截断
			leftLog := append(make([]*LogEntry, 0), rf.Logs[0])
			leftLog = append(leftLog, rf.Logs[rf.logIndex2ArrayIndex(msg.req.LastIncludedIndex)+1:]...)
			rf.Logs = leftLog
		}
	} else {
		rf.Logs = append(make([]*LogEntry, 0), rf.Logs[0])
	}
	DPrintf("Node %d install snapshot end to index %d", rf.me, msg.req.LastIncludedIndex)

	rf.Logs[0].Index = msg.req.LastIncludedIndex
	rf.Logs[0].Term = msg.req.LastIncludedTerm
	rf.snapshot = msg.req.SnapshotData
	rf.snapshotNeedApply = true

	if rf.LastApplied < msg.req.LastIncludedIndex {
		rf.LastApplied = msg.req.LastIncludedIndex
		DPrintf("Node %d lasted applied %d", rf.me, rf.LastApplied)
	}
	if rf.CommitIndex < msg.req.LastIncludedIndex {
		rf.CommitIndex = msg.req.LastIncludedIndex
		DPrintf("Node %d commit index %d", rf.me, rf.CommitIndex)
	}
}
