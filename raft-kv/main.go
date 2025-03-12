package main

import (
	"log"
	"math/rand"
	"time"

	"raft-kv/client"
	"raft-kv/raft"
)

// helper function to wait for leader confirmation by trying to submit a command.
func waitForLeader(c *client.Client) {
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		putCmd := raft.Command{Type: raft.Put, Key: "temp", Value: "temp"}
		reply, err := c.Submit(putCmd)
		if err == nil && reply.IsLeader {
			log.Printf("[TestClient] Leader confirmed at %s", c.LeaderAddress())
			return
		}
		log.Printf("[TestClient] Leader not confirmed at %s, retrying...", c.LeaderAddress())
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf("[TestClient] Could not confirm leader at %s", c.LeaderAddress())
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Define addresses for our three nodes.
	allNodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}

	// Slice to hold node instances.
	nodes := make([]*raft.RaftNode, 0)

	// Start each Raft node in a separate goroutine.
	for _, addr := range allNodes {
		// Build peers list (all addresses except self).
		peers := make([]string, 0)
		for _, p := range allNodes {
			if p != addr {
				peers = append(peers, p)
			}
		}
		// Create a new node.
		node := raft.NewRaftNode(addr, peers)
		nodes = append(nodes, node)

		// Start the RPC server for the node.
		go func(n *raft.RaftNode, address string) {
			raft.StartRPCServer(n, address)
			// Optionally, listen on the applyCh to log applied commands.
			go func(n *raft.RaftNode) {
				for res := range n.GetApplyCh() {
					log.Printf("[Node %s] Applied command: %+v", n.Id(), res)
				}
			}(n)
			// Keep the node running.
			select {}
		}(node, addr)
	}

	// Wait for the cluster to stabilize and elect a leader.
	log.Println("[Main] Waiting for cluster to stabilize...")
	time.Sleep(5 * time.Second) // Increased from 3s to 5s

	// For this combined test, we assume the leader is at "localhost:8001".
	// (In a production system, you would use a discovery mechanism.)
	leaderAddress := "localhost:8001"
	c := client.NewClient(leaderAddress)

	// Wait and confirm that the node at leaderAddress is indeed the leader.
	waitForLeader(c)
	if !tryAllNodesForLeader(c, allNodes) {
		log.Fatalf("[Main] Could not find a leader in the cluster after trying all nodes")
	}

	// Now, submit test client commands.

	// 1. Put command
	putCmd := raft.Command{Type: raft.Put, Key: "foo", Value: "bar"}
	putReply, err := c.Submit(putCmd)
	if err != nil {
		log.Fatalf("[TestClient] Error submitting Put command: %v", err)
	}
	log.Printf("[TestClient] Put command reply: %+v", putReply)

	// 2. Append command
	appendCmd := raft.Command{Type: raft.Append, Key: "foo", Value: "baz"}
	appendReply, err := c.Submit(appendCmd)
	if err != nil {
		log.Fatalf("[TestClient] Error submitting Append command: %v", err)
	}
	log.Printf("[TestClient] Append command reply: %+v", appendReply)

	// 3. Get command
	getCmd := raft.Command{Type: raft.Get, Key: "foo"}
	getReply, err := c.Submit(getCmd)
	if err != nil {
		log.Fatalf("[TestClient] Error submitting Get command: %v", err)
	}
	log.Printf("[TestClient] Get command reply: %+v", getReply)

	// Keep the main process running.
	select {}
}

// Try to find a leader by connecting to each node in sequence
func tryAllNodesForLeader(c *client.Client, nodes []string) bool {
	// First try with the current leader address
	putCmd := raft.Command{Type: raft.Put, Key: "temp", Value: "temp"}
	reply, err := c.Submit(putCmd)
	if err == nil && reply.IsLeader {
		log.Printf("[Main] Found leader at %s", c.LeaderAddress())
		return true
	}

	// If that fails, try each node in the cluster
	for _, addr := range nodes {
		testClient := client.NewClient(addr)
		reply, err := testClient.Submit(putCmd)
		if err == nil && reply.IsLeader {
			// Found a leader, update our main client
			log.Printf("[Main] Found leader at %s", addr)
			*c = *testClient // Update the client to use the new leader
			return true
		}
	}

	return false
}
