package raft

import (
	"math/rand"
	"time"
)

type timerMsg struct {
	ok chan struct{}
}

func (rf *Raft) timer() {
	for !rf.killed() {
		msg := timerMsg{
			ok: make(chan struct{}),
		}
		rf.timerChan <- msg
		<-msg.ok
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) handleTimer(msg timerMsg) {
	if time.Now().After(rf.heartbeatTimeout) {
		rf.broadcastHeartbeat()
		rf.resetHeartbeatTimeout()
	}
	if time.Now().After(rf.electionTimeout) {
		rf.startElection()
		rf.resetElectionTimeout()
	}
	msg.ok <- struct{}{}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(RandomizedElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimeout() {
	rf.heartbeatTimeout = time.Now().Add(FixedHeartbeatTimeout())
}

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(150)+250) * time.Millisecond
}

func FixedHeartbeatTimeout() time.Duration {
	return time.Millisecond * 100
}
