package raft

import "time"

// NodeState represents the state of a Raft node
type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
)

// CommandType represents the type of command in the log
type CommandType string

const (
	Put    CommandType = "Put"
	Append CommandType = "Append"
	Get    CommandType = "Get"
)

// LogEntry represents an entry in the Raft log
type LogEntry struct {
	Term    int         // The term when this entry was added
	Command CommandType // The command type (Put, Append, etc.)
	Key     string      // The key for the command
	Value   string      // The value for the command (for Put and Append)
}

// RequestVoteArgs is the argument for the RequestVote RPC
type RequestVoteArgs struct {
	Term         int    // Candidate's term
	CandidateID  string // Candidate requesting the vote
	LastLogIndex int    // Index of candidate's last log entry
	LastLogTerm  int    // Term of candidate's last log entry
}

// RequestVoteReply is the reply for the RequestVote RPC
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True if candidate received the vote
}

// AppendEntriesArgs is the argument for the AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     string     // Leader's ID
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to append (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply is the reply for the AppendEntries RPC
type AppendEntriesReply struct {
	Term    int  // Current term, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm
}

// Command represents a client command (Put, Append, Get)
type Command struct {
	Type  CommandType // Put, Append, or Get
	Key   string      // Key for the operation
	Value string      // Value for Put or Append (empty for Get)
}

// ApplyResult represents the result of applying a command to the state machine
type ApplyResult struct {
	Key   string // Key of the operation
	Value string // Resulting value (for Get), or empty for Put/Append
	Err   error  // Any error that occurred
}

// Constants for Raft configuration
const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)
