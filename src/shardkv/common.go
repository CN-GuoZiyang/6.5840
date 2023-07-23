package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrCfgNotReady = "ErrCfgNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	ClientID   int64
	SequenceID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key        string
	ClientID   int64
	SequenceID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type AcquireShardArgs struct {
	ShardIDs  []int
	ConfigNum int
}

type AcquireShardReply struct {
	Err         Err
	Store       map[int]map[string]string
	SequenceMap map[int64]int64
	ConfigNum   int
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
