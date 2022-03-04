package storage

import (
	pb "github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	"sync"
)

type Storage interface{
	GetEntries() []*pb.Entry
	AppendEntry(entry *pb.Entry)
	AppendEntriesFromIndex(x uint64, newEntries []*pb.Entry)

	GetCommitIndex() uint64
	SetCommitIndex(commitIndex uint64)

	GetLastLogIndex() uint64
	GetLastLogTerm() uint64
}

type MemoryStorage struct {
	sync.Mutex

	commitIndex uint64
	lastApplied uint64
	ents        []*pb.Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		ents: make([]*pb.Entry, 0),
	}
}

func (s *MemoryStorage) GetEntries() []*pb.Entry {
	//if s.commitIndex > 0 {
	//}
	return s.ents
}

func (s *MemoryStorage) AppendEntry(entry *pb.Entry) {
	s.ents = append(s.ents, entry)
}

func (s *MemoryStorage) AppendEntriesFromIndex(x uint64, newEntries []*pb.Entry) {
	s.ents = append(s.ents[:x], newEntries...)
}

func (s *MemoryStorage) GetCommitIndex() uint64 {
	return s.commitIndex
}

func (s *MemoryStorage) SetCommitIndex(commitIndex uint64) {
	s.commitIndex = commitIndex
}

func (s *MemoryStorage) GetLastLogIndex() uint64 {
	if len(s.ents) == 0 {
		return 0
	}
	return s.ents[len(s.ents)-1].Index
}

func (s *MemoryStorage) GetLastLogTerm() uint64 {
	if len(s.ents) == 0 {
		return 0
	}
	return s.ents[len(s.ents)-1].Term
}
