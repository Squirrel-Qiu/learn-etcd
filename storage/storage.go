package storage

import (
	pb "github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	"sync"
)

type Storage interface{
	Entries() []*pb.Entry
	GetCommitIndex() uint32
	SetCommitIndex(commitIndex uint32)
}

type MemoryStorage struct {
	sync.Mutex

	commitIndex uint32
	lastApplied uint32
	ents        []*pb.Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents: make([]*pb.Entry, 1),
	}
}

func (s *MemoryStorage) Entries() []*pb.Entry {
	if s.commitIndex > 0 {
		return s.ents
	}
	return nil
}

func (s *MemoryStorage) GetCommitIndex() uint32 {
	return s.commitIndex
}

func (s *MemoryStorage) SetCommitIndex(commitIndex uint32) {
	s.commitIndex = commitIndex
}
