package storage

import (
	pb "github.com/Squirrel-Qiu/learn-etcd/raft/raftpb"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"log"
	"strconv"
	"sync"
)

type LogStorage interface {
	GetAllEntries() []*pb.Entry
	GetEntriesFromCommitIndex() []*pb.Entry
	AppendEntry(entry *pb.Entry)
	AppendEntriesFromIndex(x uint64, newEntries []*pb.Entry)

	GetCommitIndex() uint64
	SetCommitIndex(commitIndex uint64)

	GetLastLogIndex() uint64
	GetLastLogTerm() uint64
}

type RaftLogImpl struct {
	db *bolt.DB

	sync.Mutex

	commitIndex uint64
	lastApplied uint64

	lastLogIndex uint64
	lastLogTerm  uint64

	ents []*pb.Entry
}

func NewRaftLog(db *bolt.DB) *RaftLogImpl {
	return &RaftLogImpl{
		db:   db,
		ents: make([]*pb.Entry, 0),
	}
}

func (s *RaftLogImpl) GetAllEntries() []*pb.Entry {
	s.ents = s.ents[:0]

	s.db.View(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("logs"))
		if err != nil {
			return err
		}

		for i := uint64(1); i < s.commitIndex; i++ {
			k := strconv.FormatUint(i, 10)
			v := bucket.Get([]byte(k))

			entry := &pb.Entry{}
			if err2 := proto.Unmarshal(v, entry); err2 != nil {
				log.Fatalf("parse entry failed: %v", err2)
			}
			s.ents = append(s.ents, entry)
		}
		return nil
	})
	return s.ents
}

func (s *RaftLogImpl) GetEntriesFromCommitIndex() []*pb.Entry {
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		bucket.Get([]byte(""))
		return nil
	})
	return s.ents
}

func (s *RaftLogImpl) AppendEntry(entry *pb.Entry) {
	s.Lock()
	defer s.Unlock()
	s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("logs"))
		if err != nil {
			return err
		}

		s.lastLogIndex += 1
		s.lastLogTerm = entry.Term
		entry.Index = s.lastLogIndex

		value, err2 := proto.Marshal(entry)
		if err2 != nil {
			log.Fatalf("marshal entry failed: %v", err2)
		}

		k := strconv.FormatUint(s.lastLogIndex, 10)
		if err3 := bucket.Put([]byte(k), value); err3 != nil {
			return err3
		}

		return nil
	})
}

func (s *RaftLogImpl) AppendEntriesFromIndex(x uint64, newEntries []*pb.Entry) {
	s.ents = append(s.ents[:x], newEntries...)
}

func (s *RaftLogImpl) GetCommitIndex() uint64 {
	return s.commitIndex
}

func (s *RaftLogImpl) SetCommitIndex(commitIndex uint64) {
	s.commitIndex = commitIndex
}

func (s *RaftLogImpl) GetLastLogIndex() uint64 {
	if len(s.ents) == 0 {
		return 0
	}
	return s.ents[len(s.ents)-1].Index
}

func (s *RaftLogImpl) GetLastLogTerm() uint64 {
	if len(s.ents) == 0 {
		return 0
	}
	return s.ents[len(s.ents)-1].Term
}
