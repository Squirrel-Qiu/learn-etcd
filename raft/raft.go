package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const DeLog = 1

type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	Dead
)

type raft struct {
	mu sync.Mutex

	id    int
	state StateType

	Term     int
	VotedFor int
	peerIds  []int

	server *Server

	//raftLog *raftLog

	heartbeatTimeout   time.Duration
	electionResetEvent time.Time
}

func newRaft(id int, peerIds []int, server *Server, ready <-chan struct{}) *raft {
	r := &raft{
		id:               id,
		state:            StateFollower,
		VotedFor:         -1,
		peerIds:          peerIds,
		server:           server,
		heartbeatTimeout: 50 * time.Millisecond,
	}

	go func() {
		<-ready
		r.mu.Lock()
		r.electionResetEvent = time.Now()
		r.mu.Unlock()
		r.runElectionTimeout()
	}()

	return r
}

func (r *raft) dlog(format string, args ...interface{}) {
	if DeLog == 1 {
		sprintf := fmt.Sprintf("[%d]", r.id) + format
		fmt.Printf(sprintf, args...)
	}
}

// 状态为Candidate或Follower时的选举定时器
/*
	出现以下情况跳出循环：
	1.状态改变
	2.任期改变??
	3.超时
*/
func (r *raft) runElectionTimeout() {
	timeoutDuration := r.electionTimeout()
	r.mu.Lock()
	termStarted := r.Term
	r.mu.Unlock()
	r.dlog("任期%d, 时长%v的选举定时器启动", termStarted, timeoutDuration)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		r.mu.Lock()
		if r.state != StateFollower && r.state != StateCandidate {
			r.dlog("选举定时器的服务器状态改变 state=%s, 退出", r.state)
			r.mu.Unlock()
			return
		}

		// 收到心跳任期改变??
		if termStarted != r.Term {
			r.dlog("选举定时器的term从 %d 变为 %d, 退出", termStarted, r.Term)
			r.mu.Unlock()
			return
		}

		// 超时
		if time.Since(r.electionResetEvent) >= timeoutDuration {
			r.dlog("")
			r.startElection()
			r.mu.Unlock()
			return
		}
	}
}

// 生成伪随机的定时器
func (r *raft) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// 成为candidate开始选举运动 (lock状态进入的??)
/*
	等待RequestVoteReply的过程中可能出现以下情况：
	1.状态改变
	2.对方任期更大变为Follower
	3.票数过半变为领导者
*/
func (r *raft) startElection() {
	r.state = StateCandidate
	r.Term += 1
	termStarted := r.Term
	r.VotedFor = r.id
	r.electionResetEvent = time.Now()
	r.dlog("成为候选人 term=%d , 开始发起选举运动", termStarted)

	var votesReceived int32

	for _, v := range r.peerIds {
		go func(peerId int) {
			args := &RequestVoteArgs{
				Term:        termStarted,
				CandidateId: peerId,
			}

			var reply RequestVoteReply

			r.dlog("向服务器 %d 发送RequestVote请求", peerId)
			if err := r.server.Call(peerId, "Raft.RequestVote", args, reply); err == nil {

				if r.state != StateCandidate {
					r.dlog("等待其他节点的RequestVoteReply时, 状态变为 %v", r.state)
					return
				}

				if reply.Term > termStarted {
					r.dlog("收到RequestVoteReply的任期比当前任期高")
					r.becomeFollower(reply.Term)
					return
				} else if reply.Term == termStarted {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes > (len(r.peerIds)+1)/2 {
							r.dlog("以 %d 票数当选Leader", votes)
							r.becomeLeader()
							return
						}
					}
				}
			}
		}(v)
	}

	// 给peerIds发送完RequestVote后开启定时器
	go r.runElectionTimeout()
}

func (r *raft) becomeFollower(term int) {
	r.state = StateFollower
	r.Term = term
	r.VotedFor = -1
	r.electionResetEvent = time.Now()
}

func (r *raft) becomeLeader() {
	r.state = StateLeader
	r.dlog("成为Leader, term=%d", r.Term)

	go func() {
		ticker := time.NewTicker(r.heartbeatTimeout)
		defer ticker.Stop()

		for {
			r.sendHeartbeats()
			<-ticker.C

			if r.state != StateLeader {
				return
			}
		}
	}()
}

func (r *raft) sendHeartbeats() {
	for _, v := range r.peerIds {
		go func() {
			send
		}()
	}
}
