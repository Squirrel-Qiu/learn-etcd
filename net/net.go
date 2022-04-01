package net

import (
	"context"
	"github.com/Squirrel-Qiu/learn-etcd/ch_request"
	pb "github.com/Squirrel-Qiu/learn-etcd/proto"
)

type Rpc struct {
	voteReqCh  chan ch_request.VoteReqStruct
	entryReqCh chan ch_request.EntryStruct
}

func NewRpc(voteReqCh chan ch_request.VoteReqStruct, entryReqCh chan ch_request.EntryStruct) *Rpc {
	return &Rpc{voteReqCh: voteReqCh, entryReqCh: entryReqCh}
}

func (g *Rpc) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, err error) {
	vq := ch_request.VoteReqStruct{
		Args:       args,
		VoteRespCh: make(chan *pb.RequestVoteReply, 1),
	}

	g.voteReqCh <- vq

	reply = <-vq.VoteRespCh

	return reply, nil
}

func (g *Rpc) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, err error) {
	ae := ch_request.EntryStruct{
		Args:        args,
		EntryRespCh: make(chan *pb.AppendEntriesReply, 1),
	}

	g.entryReqCh <- ae

	reply = <-ae.EntryRespCh

	return reply, nil
}
