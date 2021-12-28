package raft

import (
	"net/rpc"
	"sync"
)

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	r *raft

	rpcServer *rpc.Server
	peerClients map[int]*rpc.Client

	ready <-chan struct{}
}

func NewServer(serverId int, peerIds []int, ready <-chan struct{}) *Server {
	s := &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		ready:       ready,
	}
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.r = newRaft(s.serverId, s.peerIds, s, s.ready)

	s.rpcServer = rpc.NewServer()
	s.rpcServer.RegisterName("Raft", s.r)
	s.mu.Unlock()
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	1
}
