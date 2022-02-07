package raft

import (
	pb "github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

type Server struct {
	mu sync.Mutex

	serverId uint32
	//peerIds  []uint32

	r *raft

	rpcServer *grpc.Server
	listener  net.Listener

	peerIds     []uint32
	peerClients map[uint32]*grpc.ClientConn

	ready <-chan struct{}
	quit  chan struct{}

	wg sync.WaitGroup
}

func NewServer(serverId uint32, peerIds []uint32, ready <-chan struct{}) *Server {
	s := &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[uint32]*grpc.ClientConn),
		ready:       ready,
		quit:        make(chan struct{}),
	}
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.r = newRaft(s.serverId, s.peerIds, s.peerClients, s, s.ready)

	s.rpcServer = grpc.NewServer()
	pb.RegisterRaftServer(s.rpcServer, s.r)
	//s.rpcProxy = &RPCProxy{r: s.r}	// rpc.Register: method has 1 input parameters; needs exactly three.
	//s.rpcServer.RegisterName("Raft", s.r)

	s.mu.Unlock()

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("server [%v] listening at %s", s.serverId, s.listener.Addr())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.rpcServer.Serve(s.listener); err != nil {
			log.Printf("server [%v] %v", s.serverId, err)
		}
		//for {
		//	conn, err := s.listener.Accept()
		//	if err != nil {
		//		select {
		//		case <-s.quit:
		//			return
		//		default:
		//			log.Fatal("accept error")
		//		}
		//	}
		//
		//	s.wg.Add(1)
		//	go func() {
		//		s.rpcServer.Serve(s.listener)
		//		//s.rpcServer.ServeConn(conn)
		//		s.wg.Done()
		//	}()
		//}
	}()
}

func (s *Server) ConnectToPeer(peerId uint32, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := grpc.Dial(addr.String(), grpc.WithInsecure())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.r.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}
