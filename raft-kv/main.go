package main

import (
	"math/rand"
	"raft-kv/raft"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	allNodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for _, nodeID := range allNodes {
		peers := make([]string, 0)
		for _, p := range allNodes {
			if p != nodeID {
				peers = append(peers, p)
			}
		}
		go func(id string, peers []string) {
			node := raft.NewRaftNode(id, peers)
			raft.StartRPCServer(node, id)
			select {}
		}(nodeID, peers)
	}
	time.Sleep(time.Hour)
}
