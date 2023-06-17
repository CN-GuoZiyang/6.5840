package raft

import "time"

type applyMsgsReq struct {
	ok chan applyMsgsRes
}

type applyMsgsRes struct {
	applyMsgs []ApplyMsg
}

func (rf *Raft) applier() {
	for !rf.killed() {
		req := applyMsgsReq{
			ok: make(chan applyMsgsRes),
		}
		rf.applyMsgsChan <- req
		res := <-req.ok
		for _, msg := range res.applyMsgs {
			rf.applyCh <- msg
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func (rf *Raft) getApplyMsgs(msg applyMsgsReq) {
	var applyMsgs []ApplyMsg
	defer func() {
		msg.ok <- applyMsgsRes{applyMsgs: applyMsgs}
	}()
	if rf.waitingSnapshot != nil {
		applyMsgs = append(applyMsgs, ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.waitingSnapshot,
			SnapshotIndex: rf.Logs[0].Index,
			SnapshotTerm:  rf.Logs[0].Term,
		})
		rf.waitingSnapshot = nil
	} else {
		for rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			l, inSnapshot := rf.getLog(rf.LastApplied)
			if inSnapshot {
				continue
			}
			applyMsgs = append(applyMsgs, ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: l.Index,
			})
		}
	}
}
