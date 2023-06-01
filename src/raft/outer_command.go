package raft

type outerCommandMsg struct {
	command interface{}
	ok      chan outerCommandRes
}

type outerCommandRes struct {
	index    int
	term     int
	isLeader bool
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// Start 方法模拟一个外部 command 被提交到本台机器上
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.Status != Leader {
		return 0, 0, false
	}
	msg := outerCommandMsg{
		command: command,
		ok:      make(chan outerCommandRes),
	}
	DPrintf("node %d handle start\n", rf.me)
	rf.outerCommandChan <- msg
	res := <-msg.ok
	return res.index, res.term, res.isLeader
}

func (rf *Raft) handleOuterCommand(msg outerCommandMsg) {
	defer rf.broadcastHeartbeat()
	res := outerCommandRes{
		index:    rf.getLatestLog().Index + 1,
		term:     rf.CurrentTerm,
		isLeader: rf.Status == Leader,
	}
	defer func() {
		msg.ok <- res
	}()
	if !res.isLeader {
		DPrintf("node %d abandon outer command index %d\n", rf.me, res.index)
		return
	}
	DPrintf("node %d handle outer command index %d\n", rf.me, res.index)
	rf.Logs = append(rf.Logs, &LogEntry{Index: res.index, Term: res.term, Command: msg.command})
	rf.MatchIndex[rf.me] = len(rf.Logs)
}
