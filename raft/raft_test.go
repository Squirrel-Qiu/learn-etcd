package raft

import (
	"testing"
	"time"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
	time.Sleep(10 * time.Second)
}
