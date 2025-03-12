package raft

import (
	"log"
	"net"
	"net/rpc"
)

func StartRPCServer(node *RaftNode, address string) {
	server := rpc.NewServer()
	if err := server.Register(node); err != nil {
		log.Fatal("Failed to register RPC server:", err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("Failed to start RPC server:", err)
	}
	log.Printf("[Node %s] RPC server started on %s", node.id, address)
	go server.Accept(listener)
}
