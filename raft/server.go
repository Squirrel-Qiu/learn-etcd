package raft

import (
	"github.com/Squirrel-Qiu/learn-etcd/ch_request"
	"github.com/Squirrel-Qiu/learn-etcd/gate"
	net2 "github.com/Squirrel-Qiu/learn-etcd/net"
	"github.com/Squirrel-Qiu/learn-etcd/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"sync"
)

type Server struct {
	mu sync.Mutex

	serverId uint64

	r  *raft
	g  *net2.Rpc
	ga *gate.Gate

	rpcServer *grpc.Server
	listener  net.Listener

	peerIds     []uint64
	peerClients map[uint64]*grpc.ClientConn

	ready <-chan struct{}
	quit  chan struct{}

	wg sync.WaitGroup
}

func NewServer(serverId uint64, peerIds []uint64, ready <-chan struct{}) *Server {
	s := &Server{
		serverId:    serverId,
		peerIds:     peerIds,
		peerClients: make(map[uint64]*grpc.ClientConn),
		ready:       ready,
		quit:        make(chan struct{}),
	}

	voteReqCh := make(chan ch_request.VoteReqStruct, 1)
	entryReqCh := make(chan ch_request.EntryStruct, 1)

	getDataCh := make(chan ch_request.GetDataStruct, 1)
	deleteDataCh := make(chan ch_request.DeleteDataStruct, 1)
	updateDataCh := make(chan ch_request.UpdateDataStruct, 1)

	s.r = newRaft(s.serverId, s.peerIds, s.peerClients, s, s.ready, voteReqCh, entryReqCh, getDataCh, deleteDataCh, updateDataCh)
	s.g = net2.NewRpc(voteReqCh, entryReqCh)
	s.ga = gate.NewGate(getDataCh, deleteDataCh, updateDataCh)

	s.rpcServer = grpc.NewServer()
	proto.RegisterRPCommServer(s.rpcServer, s.g)
	proto.RegisterGateServer(s.rpcServer, s.ga)

	return s
}

func (s *Server) Serve() {
	var err error
	addr := "127.0.0.1:4314" + strconv.FormatUint(s.serverId, 10)
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
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

func (s *Server) ConnectToPeer(peerId uint64, addr net.Addr) error {
	//s.mu.Lock()
	//defer s.mu.Unlock()
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
	//s.mu.Lock()
	//defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) DisconnectAll() {
	//s.mu.Lock()
	//defer s.mu.Unlock()
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
