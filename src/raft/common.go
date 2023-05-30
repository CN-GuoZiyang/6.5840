package raft

import (
	"math/rand"
	"time"
)

// 检查 rpc 请求响应中的 term，如果大于自己的则需要更新任期并成为 Follower
func (rf *Raft) rpcTermCheck(msgTerm int) {
	if rf.CurrentTerm < msgTerm {
		rf.CurrentTerm = msgTerm
		rf.Status = Follower
		rf.VotedFor = -1
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(150)+300) * time.Millisecond
}

func FixedHeartbeatTimeout() time.Duration {
	return time.Millisecond * 100
}
