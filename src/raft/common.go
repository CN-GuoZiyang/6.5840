package raft

// 检查 rpc 请求响应中的 term，如果大于自己的则需要更新任期并成为 Follower
func (rf *Raft) rpcTermCheck(msgTerm int) bool {
	if rf.CurrentTerm < msgTerm {
		rf.CurrentTerm = msgTerm
		rf.Status = Follower
		rf.VotedFor = -1
		DPrintf("node %d become follower for term %d\n", rf.me, rf.CurrentTerm)
		rf.persist()
		return false
	}
	return true
}

func IfElseInt(cond bool, a int, b int) int {
	if cond {
		return a
	}
	return b
}
