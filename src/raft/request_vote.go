package raft

/********* 拉票请求相关数据结构 *********/
// 拉票 RPC 请求
type RequestVoteArgs struct {
	// Term Candidate 的任期
	Term int
	// CandidateId 拉票的 Candidate 的 ID
	CandidateId int
	// LastLogIndex Candidate 最后一条日志序列的索引
	LastLogIndex int
	// LastLogTerm Candidate 最后一条日志序列的任期
	LastLogTerm int
}

// 拉票 RPC 响应
type RequestVoteReply struct {
	// Term 当前任期
	Term int
	// VoteGranted true 则拉票成功
	VoteGranted bool
}

// 一场选举的元信息
type ElectionMeta struct {
	term int
	yeas int
	nays int
}

// 发送拉票请求的协程与主协程的通信消息（发送端内部消息）
type RequestVoteResMsg struct {
	resp *RequestVoteReply
	meta *ElectionMeta
}

// 接受拉票请求的协程与主协程的通信消息（接收端内部消息）
type RequestVoteMsg struct {
	req *RequestVoteArgs
	ok  chan RequestVoteReply
}

/********* 拉票请求发送端相关方法 *********/
// 拉票请求 RPC 发送入口
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送拉票请求的协程
func (rf *Raft) sendRequestVoteRoutine(peer int, args RequestVoteArgs, electionMeta *ElectionMeta) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, &args, &reply)
	if !ok {
		return
	}
	msg := RequestVoteResMsg{
		resp: &reply,
		meta: electionMeta,
	}
	rf.requestVoteResChan <- msg
}

// 主协程处理拉票请求返回结果
func (rf *Raft) handleRequestVoteRes(msg RequestVoteResMsg) {
	meta := msg.meta
	if rf.Status != Candidate {
		return
	}
	if rf.CurrentTerm != meta.term {
		return
	}
	if msg.resp.VoteGranted {
		meta.yeas++
		if meta.yeas > len(rf.peers)/2 {
			// fmt.Printf("server %d become leader for term %d\n", rf.me, rf.CurrentTerm)
			rf.Status = Leader
			rf.NextIndex = make([]int, len(rf.peers))
			rf.MatchIndex = make([]int, len(rf.peers))
			if len(rf.Logs) != 0 {
				for i := range rf.NextIndex {
					rf.NextIndex[i] = rf.getLatestLog().Index + 1
				}
			}
			resetTimer(rf.heartbeatTimer, FixedHeartbeatTimeout())
			rf.broadcastHeartbeat()
		}
	} else {
		meta.nays++
		rf.rpcTermCheck(msg.resp.Term)
		if meta.nays > len(rf.peers)/2 {
			// 反对票超过一半，则该任期选举失败；可以给其他机器投票
			rf.VotedFor = -1
		}
	}
}

/********* 拉票请求接收端相关方法 *********/
// 拉票请求 RPC 接收入口
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	msg := RequestVoteMsg{
		req: args,
		ok:  make(chan RequestVoteReply),
	}
	rf.requestVoteChan <- msg
	resp := <-msg.ok
	*reply = resp
}

// 主协程处理拉票请求
func (rf *Raft) handleRequestVote(msg RequestVoteMsg) {
	req := msg.req
	if req.Term < rf.CurrentTerm ||
		(req.Term == rf.CurrentTerm && rf.VotedFor != -1 && rf.VotedFor != req.CandidateId) {
		msg.ok <- RequestVoteReply{
			Term:        rf.CurrentTerm,
			VoteGranted: false,
		}
		return
	}
	rf.rpcTermCheck(req.Term)
	rf.VotedFor = req.CandidateId
	resetTimer(rf.electionTimer, RandomizedElectionTimeout())
	// fmt.Printf("server %d vote for server %d for term %d\n", rf.me, msg.req.CandidateId, req.Term)
	msg.ok <- RequestVoteReply{
		Term:        rf.CurrentTerm,
		VoteGranted: true,
	}
}
