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
	if msg.index <= rf.Logs[0].Index {
		// 旧快照
		return
	}
	for index, log := range rf.Logs {
		if log.Index != index {
			continue
		}
		rf.Logs[0].Index = index
		rf.Logs[0].Term = log.Term
		rf.Logs = append(rf.Logs[0:1], rf.Logs[index+1:]...)
		break
	}
	rf.SnapshotData = msg.snapshot
	rf.persist()
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

/********* 安装快照请求发送端相关方法 *********/
// 安装快照 RPC 发送入口
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	defer func() {
		msg.ok <- InstallSnapshotReply{Term: rf.CurrentTerm}
	}()

	if rf.CurrentTerm > msg.req.Term {
		// 小于当前任期，直接返回
		return
	}
	if msg.req.LastIncludedIndex <= rf.Logs[0].Index {
		// 旧快照，无需处理
		return
	}

	defer rf.persist()
	for index, log := range rf.Logs {
		if log.Index != msg.req.LastIncludedIndex {
			continue
		}
		rf.Logs = append(rf.Logs[0:1], rf.Logs[index+1:]...)
		break
	}
	rf.Logs[0].Index = msg.req.LastIncludedIndex
	rf.Logs[0].Term = msg.req.LastIncludedTerm
	rf.SnapshotData = msg.req.SnapshotData
	if rf.LastApplied < msg.req.LastIncludedIndex {
		rf.LastApplied = msg.req.LastIncludedIndex
	}
	if rf.CommitIndex < msg.req.LastIncludedIndex {
		rf.CommitIndex = msg.req.LastIncludedIndex
	}
}
