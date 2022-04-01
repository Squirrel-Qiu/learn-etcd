package ch_request

import (
	pb "github.com/Squirrel-Qiu/learn-etcd/proto"
)

type VoteReqStruct struct {
	Args       *pb.RequestVoteArgs
	VoteRespCh chan *pb.RequestVoteReply
}

type EntryStruct struct {
	Args        *pb.AppendEntriesArgs
	EntryRespCh chan *pb.AppendEntriesReply
}

type GetDataStruct struct {
	Key   []byte
	Value chan []byte
}

type DeleteDataStruct struct {
	Key     []byte
	Success chan bool
}

type UpdateDataStruct struct {
	Data    *pb.ClientData
	Success chan bool
}
