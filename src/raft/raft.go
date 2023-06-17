package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sort"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志条目
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type ServerStatus uint8

const (
	Follower  ServerStatus = 0
	Candidate ServerStatus = 1
	Leader    ServerStatus = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Status
	Status ServerStatus

	/***** 所有 Server 都包含的持久状态 *****/
	// CurrentTerm 机器遇到的最大的任期，启动时初始化为 0，单调递增
	CurrentTerm int
	// VotedFor 当前任期内投票的 Candidate ID，未投票则为 -1
	VotedFor int
	// Logs 日志条目，每个条目都包含了一条状态机指令和 Leader 接收该条目时的任期，index 从 1 开始
	Logs []*LogEntry

	/***** 所有 Server 都包含的可变状态 *****/
	// CommitIndex 已知的最大的即将提交的日志索引，启动时初始化为 0，单调递增
	CommitIndex int
	// LastApplied 最大的已提交的日志索引，启动时初始化为 0，单调递增
	LastApplied int
	// snapshot
	snapshot          []byte
	snapshotNeedApply bool

	/******* Leader 包含的可变状态，选举后初始化 *******/
	// NextIndex 每台机器下一个要发送的日志条目的索引，初始化为 Leader 最后一个日志索引 +1
	NextIndex []int
	// MatchIndex 每台机器已知复制的最高的日志条目，初始化为 0，单调递增
	MatchIndex []int

	// 定时器
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// 处理 rpc 请求的管道
	requestVoteChan     chan RequestVoteMsg
	appendEntriesChan   chan AppendEntriesMsg
	installSnapshotChan chan InstallSnapshotMsg

	// 与拉票协程通信的管道
	requestVoteResChan chan RequestVoteResMsg
	// 与追加协程通信的管道
	appendEntriesResChan chan AppendEntriesResMsg
	// 与安装快照协程通信的管道
	installSnapshotResChan chan InstallSnapshotResMsg
	// applier 获取 msg 的管道
	applyMsgsChan chan applyMsgsReq

	// 处理外部 Command 的管道
	outerCommandChan chan outerCommandMsg
	// 处理外部 Snapshot 的管道
	outerSnapshotChan chan outerSnapshotMsg
	// 外部获取服务状态的管道
	getStateChan chan getStateMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	msg := getStateMsg{
		ok: make(chan getStateRes),
	}
	rf.getStateChan <- msg
	res := <-msg.ok
	return res.term, res.isLeader
}

type getStateMsg struct {
	ok chan getStateRes
}

type getStateRes struct {
	term     int
	isLeader bool
}

// 当需持久化字段变更时，持久化数据
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
			resetTimer(rf.electionTimer, RandomizedElectionTimeout())
		case <-rf.heartbeatTimer.C:
			rf.broadcastHeartbeat()
			resetTimer(rf.heartbeatTimer, FixedHeartbeatTimeout())
		case msg := <-rf.requestVoteChan:
			rf.handleRequestVote(msg)
		case msg := <-rf.appendEntriesChan:
			rf.handleAppendEntries(msg)
		case msg := <-rf.installSnapshotChan:
			rf.handleInstallSnapshot(msg)
		case msg := <-rf.requestVoteResChan:
			rf.handleRequestVoteRes(msg)
		case msg := <-rf.appendEntriesResChan:
			rf.handleAppendEntriesRes(msg)
		case msg := <-rf.installSnapshotResChan:
			rf.handleInstallSnapshotRes(msg)
		case msg := <-rf.applyMsgsChan:
			rf.getApplyMsgs(msg)
		case msg := <-rf.outerCommandChan:
			rf.handleOuterCommand(msg)
		case msg := <-rf.outerSnapshotChan:
			rf.handleOuterSnapshot(msg)
		case msg := <-rf.getStateChan:
			msg.ok <- getStateRes{
				term:     rf.CurrentTerm,
				isLeader: rf.Status == Leader,
			}
		}
	}
}

func (rf *Raft) startElection() {
	if rf.Status == Leader {
		// leader 无需发起新选举
		return
	}
	rf.CurrentTerm += 1
	DPrintf("node %v start election for term %d!\n", rf.me, rf.CurrentTerm)
	// fmt.Printf("server %d start election for term %d\n", rf.me, rf.CurrentTerm)
	rf.Status = Candidate
	rf.VotedFor = rf.me
	rf.persist()
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLatestIndex(),
		LastLogTerm:  rf.getLatestTerm(),
	}
	meta := ElectionMeta{
		term: rf.CurrentTerm,
		yeas: 1,
		nays: 0,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVoteRoutine(peer, args, &meta)
	}
}

// broadcastHeartbeat 发送心跳包，必须在主协程下调用
func (rf *Raft) broadcastHeartbeat() {
	if rf.Status != Leader || rf.killed() {
		return
	}
	// fmt.Printf("server %d broadcast heartbeat\n", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			resetTimer(rf.electionTimer, RandomizedElectionTimeout())
			rf.MatchIndex[peer] = rf.getLatestIndex()
			continue
		}

		snapshotLog := rf.Logs[0]
		if snapshotLog.Index >= rf.NextIndex[peer] {
			// 将要发送的 Log 已经淹没在 snapshot 中了~
			DPrintf("Leader %d: Node %d left too behind! si %d vs ni %d", rf.me, peer, snapshotLog.Index, rf.NextIndex[peer])
			go rf.sendInstallSnapshotRoutine(peer, snapshotLog)
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			LeaderCommit: rf.CommitIndex,
		}
		if snapshotLog.Index == rf.NextIndex[peer] {
			args.PrevLogIndex, args.PrevLogTerm = snapshotLog.Index, snapshotLog.Term
		} else {
			log, inSnapshot := rf.getLog(rf.NextIndex[peer] - 1)
			if inSnapshot {
				log = rf.Logs[0]
			}
			args.PrevLogIndex, args.PrevLogTerm = log.Index, log.Term
		}
		startLog, _ := rf.getLog(rf.NextIndex[peer])
		if startLog != nil {
			args.Entries = rf.Logs[rf.logIndex2ArrayIndex(startLog.Index):]
			DPrintf("Leader %d: Sync Node %d Log From %d:%v to %d:%v", rf.me, peer, startLog.Index, startLog, len(rf.Logs)-1, rf.Logs[len(rf.Logs)-1])
		}
		go rf.sendAppendEntriesRoutine(peer, args)
	}
}

func (rf *Raft) logIndex2ArrayIndex(index int) int {
	return index - rf.Logs[0].Index
}

func (rf *Raft) getLog(index int) (log *LogEntry, inSnapshot bool) {
	i := rf.logIndex2ArrayIndex(index)
	if i <= 0 {
		inSnapshot = true
		return
	}
	if len(rf.Logs) <= i {
		return
	}
	log = rf.Logs[i]
	return
}

func (rf *Raft) getLatestIndex() int {
	return rf.Logs[len(rf.Logs)-1].Index
}

func (rf *Raft) getLatestTerm() int {
	return rf.Logs[len(rf.Logs)-1].Term
}

// func (rf *Raft) getLatestLog() *LogEntry {
// 	return rf.Logs[len(rf.Logs)-1]
// }

func (rf *Raft) judgetCommit() {
	// 判断是否有 Log 已经达成共识
	var matchIndexes []int
	matchIndexes = append(matchIndexes, rf.MatchIndex...)
	sort.Ints(matchIndexes)
	allAgree := matchIndexes[len(matchIndexes)/2]
	if allAgree <= rf.CommitIndex {
		return
	}
	allAgreeLog, inSnapshot := rf.getLog(allAgree)
	if inSnapshot || allAgreeLog.Term == rf.CurrentTerm {
		// allAgree 位于 snapshot 内 || 只有当前任期的日志才需要当前 Server 提交
		DPrintf("Leader %d: Index %d reach agree!\n", rf.me, allAgree)
		defer rf.broadcastHeartbeat()
		rf.CommitIndex = allAgree
		return
	}
}

func (rf *Raft) applySnapshot() {
	snapshotLog := rf.Logs[0]
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  snapshotLog.Term,
		SnapshotIndex: snapshotLog.Index,
	}
	rf.LastApplied = snapshotLog.Index
	DPrintf("Node %d: apply start", rf.me)
	rf.applyCh <- applyMsg
	DPrintf("Node %d: apply end", rf.me)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.Status = Follower
	rf.VotedFor = -1
	rf.NextIndex = make([]int, len(rf.peers))
	for index := range rf.NextIndex {
		rf.NextIndex[index] = 1
	}
	rf.MatchIndex = make([]int, len(rf.peers))
	// 0 位置占位，index 从 1 开始
	rf.Logs = append(rf.Logs, &LogEntry{})

	// Your initialization code here (2A, 2B, 2C).

	rf.electionTimer = time.NewTimer(RandomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(FixedHeartbeatTimeout())

	rf.requestVoteChan = make(chan RequestVoteMsg)
	rf.appendEntriesChan = make(chan AppendEntriesMsg)
	rf.installSnapshotChan = make(chan InstallSnapshotMsg)
	rf.requestVoteResChan = make(chan RequestVoteResMsg)
	rf.appendEntriesResChan = make(chan AppendEntriesResMsg)
	rf.installSnapshotResChan = make(chan InstallSnapshotResMsg)
	rf.applyMsgsChan = make(chan applyMsgsReq)
	rf.outerCommandChan = make(chan outerCommandMsg)
	rf.outerSnapshotChan = make(chan outerSnapshotMsg)
	rf.getStateChan = make(chan getStateMsg)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.LastApplied = rf.Logs[0].Index

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to start apply logs
	go rf.applier()

	DPrintf("node %d start", me)
	return rf
}
