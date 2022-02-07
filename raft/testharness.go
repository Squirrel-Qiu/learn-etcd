package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())
}

type Harness struct {
	// cluster是集群中所有raft服务器的列表
	cluster []*Server

	// connected为集群中的每个服务器维护了一个bool值，表明该服务器当前是否连接到对应的同伴服务器
	// （如果为false，表明服务器间存在网络分区，两者之间不会有消息往来）
	connected []bool

	n int
	t *testing.T
}

// NewHarness creates a new test Harness, initialized with n servers connected
// to each other.
// NewHarness创建了一个新的测试框架，会初始化n个相互连接的服务器
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n+1)
	connected := make([]bool, n+1)
	ready := make(chan struct{})

	// Create all Servers in this cluster, assign ids and peer ids.
	// 创建集群中的所有机器，分配ID和同伴ID
	for i := 1; i <= n; i++ {
		peerIds := make([]uint32, 0)
		for p := 1; p <= n; p++ {
			if p != i {
				peerIds = append(peerIds, uint32(p))
			}
		}

		ns[i] = NewServer(uint32(i), peerIds, ready)
		ns[i].Serve()
	}

	// Connect all peers to each other.
	// 将所有服务器进行互连
	for i := 1; i <= n; i++ {
		for j := 1; j <= n; j++ {
			if i != j {
				ns[i].ConnectToPeer(uint32(j), ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Harness{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (h *Harness) CheckSingleLeader() (leaderId uint32, leaderTerm uint32) {
	for r := 1; r <= 5; r++ {
		//leaderTerm := 0
		for i := 1; i <= h.n; i++ {
			if h.connected[i] {
				id, term, isLeader := h.cluster[i].r.Report()
				if isLeader {
					if id > 0 {
						leaderId = uint32(i)
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 1 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return 0, 0
}

func (h *Harness) Shutdown() {
	for i := 1; i <= h.n; i++ {
		// disconnect all clients connect with the server
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 1; i <= h.n; i++ {
		// shutdown the server
		h.cluster[i].Shutdown()
	}
}
