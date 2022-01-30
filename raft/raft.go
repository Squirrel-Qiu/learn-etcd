package raft

import (
	"context"
	"fmt"
	pb "github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	"github.com/Squirrel-Qiu/learn-etcd/storage"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const DeLog = 1
const None uint32 = 0

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	Dead
)

type raft struct {
	mu sync.Mutex

	id    uint32
	state StateType

	Term        uint32
	VotedFor    uint32
	peerClients map[uint32]*grpc.ClientConn

	server *Server

	sto storage.Storage

	heartbeatTimeout   time.Duration
	electionResetEvent time.Time

	nextIndex  map[uint32]uint32
	matchIndex map[uint32]uint32
}

func newRaft(id uint32, peerClients map[uint32]*grpc.ClientConn, server *Server, ready <-chan struct{}) *raft {
	r := &raft{
		id:               id,
		state:            StateFollower,
		peerClients:      peerClients,
		VotedFor:         None,
		server:           server,
		heartbeatTimeout: 30 * time.Millisecond,
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

func (r *raft) Report() (id uint32, term uint32, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id, r.Term, r.state == StateLeader
}

func (r *raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = Dead
	r.dlog("becomes Dead")
}

func (r *raft) dlog(format string, args ...interface{}) {
	if DeLog == 1 {
		sprintf := fmt.Sprintf("[%d]", r.id) + format
		log.Printf(sprintf, args...)
	}
}

// 状态为Candidate或Follower时的选举定时器
/*
	出现以下情况跳出循环：
	1.状态改变
	2.收到投票请求或者心跳的任期改变
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

		// 收到心跳任期改变
		if termStarted != r.Term {
			r.dlog("选举定时器的term从 %d 变为 %d, 退出", termStarted, r.Term)
			r.mu.Unlock()
			return
		}

		// 超时
		if elapsed := time.Since(r.electionResetEvent); elapsed >= timeoutDuration {
			r.dlog("选举定时器到时, 开始新一轮选举")
			r.startElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

// 生成伪随机的定时器
func (r *raft) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// 成为candidate开始选举运动 (lock状态进入的)
/*
	等待RequestVoteReply的过程中可能出现以下情况：
	1.状态改变
	2.对方任期更大变为Follower
	3.票数过半变为领导者
*/
func (r *raft) startElection() {
	r.state = StateCandidate
	r.Term += 1
	currentTerm := r.Term
	r.VotedFor = r.id
	r.electionResetEvent = time.Now()
	r.dlog("成为候选人 term=%d , 开始发起选举运动", currentTerm)

	var votesReceived uint32

	for i := range r.peerClients {
		go func(peerId uint32) {
			args := &pb.RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: r.id,
			}
			r.dlog("向服务器 %d 发送RequestVote请求", peerId)

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
			defer cancelFunc()

			if reply, err := pb.NewRaftClient(r.peerClients[peerId]).RequestVote(ctx, args); err == nil {
				r.dlog("收到 %d RequestVoteReply的响应", peerId)
				r.mu.Lock()
				defer r.mu.Unlock()

				if r.state != StateCandidate {
					r.dlog("等待其他节点的RequestVoteReply时, 状态变为 %v", r.state)
					return
				}

				if reply.Term > currentTerm {
					r.dlog("收到RequestVoteReply的任期比当前任期高")
					r.becomeFollower(reply.Term)
					return
				} else if reply.Term == currentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddUint32(&votesReceived, 1))
						if votes > (len(r.peerClients)+1)/2 {
							r.dlog("以 %d 票数当选Leader", votes)
							r.becomeLeader()
							return
						}
					}
				}
			}
		}(i)
	}

	// 给peerIds发送完RequestVote后开启定时器
	go r.runElectionTimeout()
}

func (r *raft) becomeFollower(term uint32) {
	r.state = StateFollower
	r.Term = term
	r.VotedFor = None
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

			r.mu.Lock()
			if r.state != StateLeader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

// Leader发送心跳
func (r *raft) sendHeartbeats() {
	r.mu.Lock()
	currentTerm := r.Term
	r.mu.Unlock()

	for i := range r.peerClients {
		go func(peerId uint32) {
			r.mu.Lock()

			logs := r.sto.Entries()
			x := r.nextIndex[peerId]
			preLogIndex := x - 1
			preLogTerm := uint32(0)
			if preLogIndex > 0 {
				preLogTerm = logs[preLogIndex].Term
			}

			entries := logs[x:]
			leaderCommit := r.sto.GetCommitIndex()

			args := &pb.AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     r.id,
				PreLogIndex:  preLogIndex,
				PreLogTerm:   preLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			r.mu.Unlock()

			r.dlog("向服务器 %d 发送AppendEntries请求", peerId)

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
			defer cancelFunc()

			if reply, err := pb.NewRaftClient(r.peerClients[peerId]).AppendEntries(ctx, args); err == nil {
				r.dlog("收到 %d AppendEntriesReply的响应", peerId)
				r.mu.Lock()
				defer r.mu.Unlock()

				if reply.Term > currentTerm {
					r.dlog("收到AppendEntriesReply的任期比当前任期高")
					r.becomeFollower(reply.Term)
					return
				}

				if r.state == StateLeader && currentTerm == reply.Term {
					if reply.Success {
						r.nextIndex[peerId] = x + uint32(len(entries))
						r.matchIndex[peerId] = r.nextIndex[peerId] - 1
						r.dlog("收到 %d AppendEntries添加成功的响应: nextIndex= %v, matchIndex= %v", peerId, r.nextIndex[peerId], r.matchIndex[peerId])

						// 检查是否有可提交的指令
						for i := leaderCommit + 1; i < uint32(len(logs)); i++ {
							if logs[i].Term == currentTerm {
								matchCount := 1
								for p := range r.peerClients {
									if r.matchIndex[p] >= i {
										matchCount++
									}
								}
								if matchCount > (len(r.peerClients)+1)/2 {
									r.sto.SetCommitIndex(i)
								}
							}
						}

						// if ok commit
						if r.sto.GetCommitIndex() != leaderCommit {
							r.dlog("提交日志, Leader的commitIndex变为 %d", r.sto.GetCommitIndex())
							// TODO commit notify
						}
					} else {
						r.nextIndex[peerId] = x - 1
						r.dlog("收到 %d AppendEntries添加失败的响应: nextIndex= %v", peerId, x-1)
					}
				}
			}
		}(i)
	}
}

func (r *raft) lastLogIndexAndTerm() (uint32, uint32) {
	n := len(r.sto.Entries())
	if n > 0 {
		lastLogIndex := uint32(n - 1)
		return lastLogIndex, r.sto.Entries()[lastLogIndex].Term
	}
	return 0, 0
}

func (r *raft) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Dead {
		return nil, nil
	}
	r.dlog("收到RequestVote请求: %+v", args)

	if args.Term > r.Term {
		r.dlog("当前任期小于RequestVoteArgs的任期, 变为Follower")
		r.becomeFollower(args.Term)
	}

	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()

	reply = &pb.RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}

	// 任期相同且没有投过票或已经投过该Candidate
	if args.Term == r.Term &&
		(r.VotedFor == None || r.VotedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.VotedFor = args.CandidateId
		r.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = r.Term
	r.dlog("RequestVote应答: %+v", reply)
	return reply, nil
}

func (r *raft) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Dead {
		return nil, nil
	}
	r.dlog("收到AppendEntries请求: %+v", args)

	if args.Term > r.Term {
		r.dlog("当前任期小于AppendEntriesArgs的任期, 变为Follower")
		r.becomeFollower(args.Term)
	}

	reply = &pb.AppendEntriesReply{
		Term:    0,
		Success: false,
	}

	if args.Term == r.Term {
		// 当Leader出现时, 其他人都是Follower (Leader唯一)
		if r.state != StateFollower {
			r.becomeFollower(args.Term)
		}
		r.electionResetEvent = time.Now()

		logs := r.sto.Entries()
		// 检查本地日志在索引PreLogIndex处的任期是否与PreLogTerm匹配
		if args.PreLogIndex == 0 || (args.PreLogIndex < uint32(len(logs)) && args.PreLogTerm == logs[args.PreLogIndex].Term) {
			reply.Success = true

			// 寻找插入点
			logInsertIndex := args.PreLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= uint32(len(logs)) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if logs[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// 循环结束时:
			// - logInsertIndex指向本地日志结尾,或与Leader发送的日志(请求条目)之间存在冲突的索引位置
			// - newEntriesIndex指向请求条目开始插入的索引位置
			if newEntriesIndex < len(args.Entries) {
				r.dlog("从索引 %d 处开始插入以下日志 %v", logInsertIndex, args.Entries[newEntriesIndex:])
				r.sto.AppendEntriesFromIndex(logInsertIndex, args.Entries[newEntriesIndex:])
			}

			if args.LeaderCommit > r.sto.GetCommitIndex() {
				ci := minIndex(args.LeaderCommit, uint32(len(r.sto.Entries())))
				r.sto.SetCommitIndex(ci)
				r.dlog("commitIndex变为 %d", ci)
				// TODO
			}
		}
	}

	reply.Term = r.Term
	r.dlog("AppendEntries应答: %+v", reply)
	return reply, nil
}

func minIndex(x, y uint32) uint32 {
	if x < y {
		return x
	}
	return y
}
