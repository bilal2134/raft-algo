package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// RaftNode represents a single node in the Raft cluster.
type RaftNode struct {
	mu             sync.Mutex
	id             string
	peers          []string
	state          NodeState
	currentTerm    int
	votedFor       string
	log            []LogEntry
	commitIndex    int
	lastApplied    int
	nextIndex      map[string]int
	matchIndex     map[string]int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyResult
	voteCount      int
	kv             map[string]string // Simplified KV store for now
}

func NewRaftNode(id string, peers []string) *RaftNode {
	node := &RaftNode{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		applyCh:     make(chan ApplyResult, 100), // Add buffer to applyCh to prevent blocking
		kv:          make(map[string]string),
	}

	// Start the election timer immediately
	go func() {
		// Small delay to ensure RPC server is ready
		time.Sleep(100 * time.Millisecond)
		node.resetElectionTimer()
	}()

	return node
}

// resetElectionTimer resets the election timer with a random timeout.
func (rn *RaftNode) resetElectionTimer() {
	timeout := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	log.Printf("[Node %s] Setting election timer for %v", rn.id, timeout)
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	rn.electionTimer = time.AfterFunc(timeout, func() {
		log.Printf("[Node %s] Election timer expired, starting election", rn.id)
		go rn.startElection()
	})
}

// AppendEntries handles an incoming AppendEntries RPC.
func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	// Reject if leader’s term is stale.
	if args.Term < rn.currentTerm {
		return nil
	}

	// If term in RPC is higher, update own term and step down.
	if args.Term > rn.currentTerm {
		log.Printf("[Node %s] Updating term from %d to %d and stepping down to follower due to AppendEntries",
			rn.id, rn.currentTerm, args.Term)
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// Log consistency check:
	// If PrevLogIndex is greater than the current log length, reject.
	if args.PrevLogIndex > len(rn.log) {
		return nil
	}
	// If PrevLogIndex > 0, ensure log entry at that index matches.
	if args.PrevLogIndex > 0 {
		if rn.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Conflict detected: delete the conflicting entry and all that follow.
			rn.log = rn.log[:args.PrevLogIndex-1]
			return nil
		}
	}

	// Append any new entries not already in the log.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index <= len(rn.log) {
			// If an existing entry conflicts, delete it and all that follow.
			if rn.log[index-1].Term != entry.Term {
				rn.log = rn.log[:index-1]
				rn.log = append(rn.log, args.Entries[i:]...)
				break
			}
		} else {
			rn.log = append(rn.log, args.Entries[i:]...)
			break
		}
	}

	// Update commitIndex if leader’s commit index is higher.
	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log))
		rn.applyEntries()
	}

	reply.Success = true
	rn.resetElectionTimer() // Valid heartbeat resets election timer.
	return nil
}

func (rn *RaftNode) applyEntries() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied-1]
		log.Printf("[Node %s] Applying log entry at index %d: %+v", rn.id, rn.lastApplied, entry)

		// Apply the command to the KV store directly
		result := ApplyResult{
			Key: entry.Key,
		}

		switch entry.Command {
		case Put:
			rn.kv[entry.Key] = entry.Value
			log.Printf("[Node %s] PUT %s = %s", rn.id, entry.Key, entry.Value)
		case Append:
			rn.kv[entry.Key] += entry.Value
			log.Printf("[Node %s] APPEND %s += %s", rn.id, entry.Key, entry.Value)
		case Get:
			value, ok := rn.kv[entry.Key]
			if ok {
				result.Value = value
				log.Printf("[Node %s] GET %s = %s", rn.id, entry.Key, value)
			} else {
				log.Printf("[Node %s] GET %s: key not found", rn.id, entry.Key)
			}
		default:
			log.Printf("[Node %s] Unknown command type: %s", rn.id, entry.Command)
		}

		rn.applyCh <- result
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RequestVote handles an incoming RequestVote RPC.
func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	log.Printf("[Node %s] Received RequestVote from %s for term %d",
		rn.id, args.CandidateID, args.Term)

	if args.Term < rn.currentTerm {
		log.Printf("[Node %s] Rejecting vote for %s: candidate term %d < current term %d",
			rn.id, args.CandidateID, args.Term, rn.currentTerm)
		return nil
	}

	if args.Term > rn.currentTerm {
		log.Printf("[Node %s] Updating term from %d to %d and stepping down to follower due to RequestVote",
			rn.id, rn.currentTerm, args.Term)
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	reply.Term = rn.currentTerm

	lastLogIndex := len(rn.log)
	var lastLogTerm int
	if lastLogIndex > 0 {
		lastLogTerm = rn.log[lastLogIndex-1].Term
	} else {
		lastLogTerm = 0
	}

	if (rn.votedFor == "" || rn.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rn.votedFor = args.CandidateID
		reply.VoteGranted = true
		log.Printf("[Node %s] Granting vote to %s for term %d",
			rn.id, args.CandidateID, args.Term)
		rn.resetElectionTimer()
	} else {
		log.Printf("[Node %s] Not granting vote to %s: votedFor=%s, lastLogTerm=%d, candidateLastLogTerm=%d",
			rn.id, args.CandidateID, rn.votedFor, lastLogTerm, args.LastLogTerm)
	}

	return nil
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	currentTerm := rn.currentTerm + 1
	rn.state = Candidate
	rn.currentTerm = currentTerm
	rn.votedFor = rn.id
	rn.voteCount = 1 // Vote for self.
	rn.resetElectionTimer()
	rn.mu.Unlock()

	log.Printf("[Node %s] Starting election for term %d", rn.id, currentTerm)

	for _, peer := range rn.peers {
		go func(peer string, term int) {
			var lastLogIndex int
			var lastLogTerm int = 0

			rn.mu.Lock()
			if len(rn.log) > 0 {
				lastLogIndex = len(rn.log)
				lastLogTerm = rn.log[lastLogIndex-1].Term
			}
			rn.mu.Unlock()

			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			log.Printf("[Node %s] Sending RequestVote to %s for term %d", rn.id, peer, term)
			if rn.sendRequestVote(peer, args, &reply) {
				rn.handleVoteReply(peer, reply)
			} else {
				log.Printf("[Node %s] Failed to send RequestVote to %s", rn.id, peer)
			}
		}(peer, currentTerm)
	}
}

func (rn *RaftNode) sendAppendEntries(peer string, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		log.Println("Failed to dial peer:", peer, err)
		return false
	}
	defer client.Close()

	err = client.Call("RaftNode.AppendEntries", args, reply)
	if err != nil {
		log.Println("AppendEntries RPC failed:", err)
		return false
	}
	return true
}

func (rn *RaftNode) sendRequestVote(peer string, args RequestVoteArgs, reply *RequestVoteReply) bool {
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		log.Printf("[Node %s] Failed to dial peer %s: %v", rn.id, peer, err)
		return false
	}
	defer client.Close()

	err = client.Call("RaftNode.RequestVote", args, reply)
	if err != nil {
		log.Printf("[Node %s] RequestVote RPC to %s failed: %v", rn.id, peer, err)
		return false
	}
	log.Printf("[Node %s] Received vote response from %s: granted=%v, term=%d", rn.id, peer, reply.VoteGranted, reply.Term)
	return true
}

func (rn *RaftNode) handleVoteReply(peer string, reply RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[Node %s] Processing vote reply from %s: granted=%v, term=%d (current term: %d)",
		rn.id, peer, reply.VoteGranted, reply.Term, rn.currentTerm)

	if reply.Term > rn.currentTerm {
		log.Printf("[Node %s] Discovered higher term %d > %d, reverting to follower",
			rn.id, reply.Term, rn.currentTerm)
		rn.currentTerm = reply.Term
		rn.state = Follower
		rn.votedFor = ""
		return
	}

	if reply.VoteGranted && reply.Term == rn.currentTerm && rn.state == Candidate {
		rn.voteCount++
		majority := (len(rn.peers)+1)/2 + 1
		log.Printf("[Node %s] Vote granted from %s for term %d, vote count: %d/%d",
			rn.id, peer, rn.currentTerm, rn.voteCount, majority)
		if rn.voteCount >= majority {
			log.Printf("[Node %s] Received majority votes (%d/%d), becoming leader for term %d",
				rn.id, rn.voteCount, len(rn.peers)+1, rn.currentTerm)
			rn.becomeLeader()
		}
	} else {
		log.Printf("[Node %s] Vote not counted from %s: granted=%v, reply.Term=%d, currentTerm=%d, state=%s",
			rn.id, peer, reply.VoteGranted, reply.Term, rn.currentTerm, rn.state)
	}
}

func (rn *RaftNode) becomeLeader() {
	if rn.state != Candidate {
		return // Only candidates can become leaders.
	}

	rn.state = Leader
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = len(rn.log) + 1
		rn.matchIndex[peer] = 0
	}
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	log.Printf("[Node %s] Became leader for term %d", rn.id, rn.currentTerm)

	// Start sending heartbeats immediately.
	rn.sendHeartbeats()
	rn.startHeartbeatTimer()
}

// SubmitCommand is the RPC method that clients call to propose a new command.
func (rn *RaftNode) SubmitCommand(cmd Command, reply *SubmitCommandReply) error {
	index, term, isLeader := rn.StartCommand(cmd)
	reply.Index = index
	reply.Term = term
	reply.IsLeader = isLeader
	return nil
}

// StartCommand is called internally (or via RPC) to propose a new command.
// Returns log index, current term, and whether the node is leader.
func (rn *RaftNode) StartCommand(cmd Command) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return -1, rn.currentTerm, false
	}

	entry := LogEntry{
		Term:    rn.currentTerm,
		Command: cmd.Type,
		Key:     cmd.Key,
		Value:   cmd.Value,
	}
	rn.log = append(rn.log, entry)
	index := len(rn.log)
	log.Printf("[Node %s] Appended new command at index %d: %+v", rn.id, index, entry)
	// Trigger immediate replication.
	go rn.broadcastAppendEntries()
	return index, rn.currentTerm, true
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers.
func (rn *RaftNode) broadcastAppendEntries() {
	for _, peer := range rn.peers {
		go rn.replicateLogToPeer(peer)
	}
}

// replicateLogToPeer replicates missing log entries to a specific peer.
func (rn *RaftNode) replicateLogToPeer(peer string) {
	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
		}
		nextIdx := rn.nextIndex[peer]
		prevLogIndex := nextIdx - 1
		var prevLogTerm int
		if prevLogIndex > 0 && prevLogIndex <= len(rn.log) {
			prevLogTerm = rn.log[prevLogIndex-1].Term
		} else {
			prevLogTerm = 0
		}
		entries := []LogEntry{}
		if nextIdx-1 < len(rn.log) {
			entries = rn.log[nextIdx-1:]
		}
		args := AppendEntriesArgs{
			Term:         rn.currentTerm,
			LeaderID:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}
		rn.mu.Unlock()

		var reply AppendEntriesReply
		if rn.sendAppendEntries(peer, args, &reply) {
			rn.mu.Lock()
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.state = Follower
				rn.votedFor = ""
				rn.mu.Unlock()
				return
			}
			if reply.Success {
				// Update nextIndex and matchIndex for this peer.
				rn.nextIndex[peer] = nextIdx + len(entries)
				rn.matchIndex[peer] = rn.nextIndex[peer] - 1
				rn.updateCommitIndex()
				rn.mu.Unlock()
				return
			} else {
				// Decrement nextIndex and retry.
				if rn.nextIndex[peer] > 1 {
					rn.nextIndex[peer]--
				}
				rn.mu.Unlock()
				time.Sleep(HeartbeatInterval)
			}
		} else {
			time.Sleep(HeartbeatInterval)
		}
	}
}

// updateCommitIndex checks if any new log entries can be committed.
func (rn *RaftNode) updateCommitIndex() {
	for idx := rn.commitIndex + 1; idx <= len(rn.log); idx++ {
		count := 1 // Leader itself.
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= idx {
				count++
			}
		}
		majority := (len(rn.peers)+1)/2 + 1
		if count >= majority && rn.log[idx-1].Term == rn.currentTerm {
			rn.commitIndex = idx
			rn.applyEntries()
		}
	}
}

// startHeartbeatTimer schedules periodic heartbeats.
func (rn *RaftNode) startHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.AfterFunc(HeartbeatInterval, func() {
		rn.mu.Lock()
		if rn.state == Leader {
			rn.sendHeartbeats()
			rn.startHeartbeatTimer() // Reschedule.
		}
		rn.mu.Unlock()
	})
}

// sendHeartbeats sends periodic AppendEntries RPCs (heartbeats) to all peers.
func (rn *RaftNode) sendHeartbeats() {
	for _, peer := range rn.peers {
		go func(peer string) {
			rn.mu.Lock()
			nextIdx := rn.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			var prevLogTerm int
			if prevLogIndex > 0 && prevLogIndex <= len(rn.log) {
				prevLogTerm = rn.log[prevLogIndex-1].Term
			} else {
				prevLogTerm = 0
			}
			entries := []LogEntry{}
			if nextIdx-1 < len(rn.log) {
				entries = rn.log[nextIdx-1:]
			}
			args := AppendEntriesArgs{
				Term:         rn.currentTerm,
				LeaderID:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rn.commitIndex,
			}
			rn.mu.Unlock()

			var reply AppendEntriesReply
			rn.sendAppendEntries(peer, args, &reply)
		}(peer)
	}
}

// Id returns the node's identifier.
func (rn *RaftNode) Id() string {
	return rn.id
}

// GetApplyCh returns the channel for applied commands.
func (rn *RaftNode) GetApplyCh() chan ApplyResult {
	return rn.applyCh
}
