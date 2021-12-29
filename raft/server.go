package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	r *raft

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan struct{}
	quit  chan struct{}

	wg sync.WaitGroup
}

func NewServer(serverId int, peerIds []int, ready <-chan struct{}) *Server {
	s := &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		ready:       ready,
		quit:        make(chan struct{}),
	}
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.r = newRaft(s.serverId, s.peerIds, s, s.ready)

	s.rpcServer = rpc.NewServer()
	s.rpcServer.RegisterName("Raft", s.r)
	s.mu.Unlock()

	var err error
	s.listener, err = net.Listen("tcp", "0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("server [%v] listening at %s", s.serverId, s.listener.Addr())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error")
				}
			}

			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial("tcp", addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call %d failed after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}
