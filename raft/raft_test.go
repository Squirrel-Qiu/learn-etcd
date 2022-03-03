package raft

import (
	"log"
	"testing"
	"time"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(leaderId, []byte("squirrel-jiu"))
	h.SubmitToServer(leaderId, []byte("I'm a machine"))
	ents := h.GetLeaderEntries(leaderId)
	log.Println(ents, 6666)
	time.Sleep(150 * time.Millisecond)
}
