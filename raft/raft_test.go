package raft

import (
	"testing"
	"time"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(leaderId, []byte("squirrel-jiu"))
	h.SubmitToServer(leaderId, []byte("squirrel-jiu"))
	time.Sleep(150 * time.Millisecond)
}
