package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// RaftNode represents a single node in the Raft cluster
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
}

// NewRaftNode creates a new Raft node with the given ID and peers
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
		applyCh:     make(chan ApplyResult),
	}
	node.resetElectionTimer()
	return node
}

// resetElectionTimer resets the election timer with a random timeout
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

// AppendEntries handles an incoming AppendEntries RPC (stub)
func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	// Deny if leader's term is less than current term
	if args.Term < rn.currentTerm {
		return nil
	}

	// Step down to follower if leader's term is higher
	if args.Term > rn.currentTerm {
		log.Printf("[Node %s] Updating term from %d to %d and stepping down to follower due to AppendEntries",
			rn.id, rn.currentTerm, args.Term)
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// Acknowledge if term is valid (log consistency check to be added later)
	reply.Success = true
	log.Printf("[Node %s] Received heartbeat from leader %s for term %d", rn.id, args.LeaderID, args.Term)
	rn.resetElectionTimer() // Reset timer since valid AppendEntries received
	return nil
}

// RequestVote handles an incoming RequestVote RPC
func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Set reply term to current term
	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	// Add log message when receiving RequestVote
	log.Printf("[Node %s] Received RequestVote from %s for term %d",
		rn.id, args.CandidateID, args.Term)

	// Deny vote if candidate's term is less than current term
	if args.Term < rn.currentTerm {
		log.Printf("[Node %s] Rejecting vote for %s: candidate term %d < current term %d",
			rn.id, args.CandidateID, args.Term, rn.currentTerm)
		return nil
	}

	// Step down to follower if candidate's term is higher
	if args.Term > rn.currentTerm {
		log.Printf("[Node %s] Updating term from %d to %d and stepping down to follower due to RequestVote",
			rn.id, rn.currentTerm, args.Term)
		rn.currentTerm = args.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// Update the reply term to the updated current term
	reply.Term = rn.currentTerm

	// Determine receiver's last log index and term
	lastLogIndex := len(rn.log)
	var lastLogTerm int
	if lastLogIndex > 0 {
		lastLogTerm = rn.log[lastLogIndex-1].Term
	} else {
		lastLogTerm = 0
	}

	// Grant vote if not yet voted or voted for this candidate, and log is up-to-date
	if (rn.votedFor == "" || rn.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rn.votedFor = args.CandidateID
		reply.VoteGranted = true
		log.Printf("[Node %s] Granting vote to %s for term %d",
			rn.id, args.CandidateID, args.Term)
		rn.resetElectionTimer() // Reset election timer since vote is granted
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
	rn.voteCount = 1 // Vote for self
	rn.resetElectionTimer()
	rn.mu.Unlock()

	log.Printf("[Node %s] Starting election for term %d", rn.id, currentTerm)

	for _, peer := range rn.peers {
		go func(peer string, term int) {
			lastLogIndex := 0
			var lastLogTerm int = 0

			rn.mu.Lock()
			if len(rn.log) > 0 {
				lastLogIndex = len(rn.log) - 1
				lastLogTerm = rn.log[lastLogIndex].Term
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

// sendAppendEntries sends an AppendEntries RPC to a peer
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

// becomeLeader transitions the node to Leader state
func (rn *RaftNode) becomeLeader() {
	if rn.state != Candidate {
		return // Safety check: only candidates can become leaders
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

	// Start sending heartbeats immediately
	rn.sendHeartbeats()
	rn.startHeartbeatTimer()
}

// startHeartbeatTimer schedules periodic heartbeats
func (rn *RaftNode) startHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.AfterFunc(HeartbeatInterval, func() {
		rn.mu.Lock()
		if rn.state == Leader {
			rn.sendHeartbeats()
			rn.startHeartbeatTimer() // Schedule next heartbeat
		}
		rn.mu.Unlock()
	})
}

// sendHeartbeats sends AppendEntries RPCs (heartbeats) to all peers
func (rn *RaftNode) sendHeartbeats() {
	log.Printf("[Node %s] Sending heartbeats as leader for term %d", rn.id, rn.currentTerm)
	for _, peer := range rn.peers {
		go func(peer string) {
			nextIdx := rn.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			var prevLogTerm int
			if prevLogIndex > 0 {
				prevLogTerm = rn.log[prevLogIndex-1].Term
			} else {
				prevLogTerm = 0
			}
			args := AppendEntriesArgs{
				Term:         rn.currentTerm,
				LeaderID:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{}, // Empty for heartbeat
				LeaderCommit: rn.commitIndex,
			}

			log.Printf("[Node %s] Sending heartbeat to %s", rn.id, peer)

			var reply AppendEntriesReply
			if rn.sendAppendEntries(peer, args, &reply) {
				// Success handling if needed
			}
		}(peer)
	}
}
