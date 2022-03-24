package raft

import (
	"github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	"log"
	"testing"
	"time"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(leaderId, []raftpb.Data{{Key: []byte("squ"), Value: []byte("squirrel-qiu")},
		{Key: []byte("machine"), Value: []byte("I'm a machine")}})
	//h.SubmitToServer(leaderId, &raftpb.Data{Key: []byte("machine"), Value: []byte("I'm a machine")})
	time.Sleep(150 * time.Millisecond)
	h.SubmitToServer(leaderId, []raftpb.Data{{Key: []byte("computer"), Value: []byte("This is a computer")}})
	time.Sleep(150 * time.Millisecond)

	ents := h.GetLeaderEntries(leaderId)
	log.Println(ents, 6666)
}
