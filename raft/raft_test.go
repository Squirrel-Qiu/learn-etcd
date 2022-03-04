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
	time.Sleep(150 * time.Millisecond)
	h.SubmitToServer(leaderId, []byte("This is a computer"))
	time.Sleep(150 * time.Millisecond)
	ents := h.GetLeaderEntries(leaderId)
	log.Println(ents, 6666)
}
