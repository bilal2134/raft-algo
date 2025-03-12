package client

import (
	"log"
	"net/rpc"

	"raft-kv/raft"
)

// Client represents a simple client for the Raft cluster.
type Client struct {
	leaderAddress string
}

// NewClient creates a new client given the leader's address.
func NewClient(leaderAddress string) *Client {
	return &Client{
		leaderAddress: leaderAddress,
	}
}

// LeaderAddress returns the leader's address.
func (c *Client) LeaderAddress() string {
	return c.leaderAddress
}

// Submit sends a command (Put, Append, Get) to the Raft cluster.
func (c *Client) Submit(cmd raft.Command) (raft.SubmitCommandReply, error) {
	var reply raft.SubmitCommandReply
	client, err := rpc.Dial("tcp", c.leaderAddress)
	if err != nil {
		log.Printf("[Client] Failed to dial leader at %s: %v", c.leaderAddress, err)
		return reply, err
	}
	defer client.Close()

	err = client.Call("RaftNode.SubmitCommand", cmd, &reply)
	if err != nil {
		log.Printf("[Client] SubmitCommand RPC failed: %v", err)
		return reply, err
	}
	return reply, nil
}
