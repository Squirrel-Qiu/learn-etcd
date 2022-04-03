package storage

import (
	"log"
	"strconv"
	"time"

	pb "github.com/Squirrel-Qiu/learn-etcd/proto"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

type LogStorage interface {
	GetAllEntries() []*pb.Entry
	GetEntriesFromIndex(startIndex uint64) []*pb.Entry
	AppendEntry(entry *pb.Entry)
	AppendEntriesFromIndex(logInsertPosition uint64, newEntries []*pb.Entry)
	DeleteEntry(entry *pb.Entry) error

	GetCommitIndex() uint64
	SetCommitIndex(commitIndex uint64)

	GetLastLogIndex() uint64
	GetLastLogTerm() uint64
	GetLogTermByIndex(logIndex uint64) uint64
}

type RaftLogImpl struct {
	db *bolt.DB

	commitIndex uint64
	lastApplied uint64

	lastLogIndex uint64
	lastLogTerm  uint64
}

func NewRaftLog(db *bolt.DB) *RaftLogImpl {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("logs"))
		if err != nil {
			log.Fatalf("get logs-bucket failed: %v", err)
		}

		return nil
	})

	return &RaftLogImpl{
		db: db,
	}
}

func (s *RaftLogImpl) GetAllEntries() []*pb.Entry {
	ents := make([]*pb.Entry, 0)
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))

		for i := uint64(1); i <= s.lastLogIndex; i++ {
			k := strconv.FormatUint(i, 10)
			v := bucket.Get([]byte(k))

			entry := &pb.Entry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				log.Fatalf("parse entry failed: %v", err)
			}
			ents = append(ents, entry)
		}

		return nil
	})
	return ents
}

func (s *RaftLogImpl) GetEntriesFromIndex(startIndex uint64) []*pb.Entry {
	ents := make([]*pb.Entry, 0)
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))

		for i := startIndex; i <= s.lastLogIndex; i++ {
			k := strconv.FormatUint(i, 10)
			v := bucket.Get([]byte(k))

			entry := &pb.Entry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				log.Fatalf("parse entry failed: %v", err)
			}
			ents = append(ents, entry)
		}

		return nil
	})
	return ents
}

func (s *RaftLogImpl) AppendEntry(entry *pb.Entry) {
	s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))

		s.lastLogIndex += 1
		s.lastLogTerm = entry.Term
		entry.Index = s.lastLogIndex

		value, err2 := proto.Marshal(entry)
		if err2 != nil {
			log.Fatalf("marshal entry failed: %v", err2)
		}

		k := strconv.FormatUint(s.lastLogIndex, 10)
		if err3 := bucket.Put([]byte(k), value); err3 != nil {
			log.Fatalf("put entry into bucket failed: %v", err3)
		}

		return nil
	})
}

func (s *RaftLogImpl) AppendEntriesFromIndex(logInsertPosition uint64, newEntries []*pb.Entry) {
	updateStart := time.Now()

	s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))

		for _, ent := range newEntries {
			s.lastLogIndex = logInsertPosition + 1
			s.lastLogTerm = ent.Term

			value, err := proto.Marshal(ent)
			if err != nil {
				log.Fatalf("marshal entry failed: %v", err)
			}

			k := strconv.FormatUint(s.lastLogIndex, 10)
			if err2 := bucket.Put([]byte(k), value); err2 != nil {
				log.Fatalf("put entry into bucket failed: %v", err2)
			}
			logInsertPosition++
		}

		return nil
	})

	log.Printf("update time consuming: %s", time.Since(updateStart))
}

func (s *RaftLogImpl) DeleteEntry(entry *pb.Entry) error {
	return nil
}

func (s *RaftLogImpl) GetCommitIndex() uint64 {
	return s.commitIndex
}

func (s *RaftLogImpl) SetCommitIndex(commitIndex uint64) {
	s.commitIndex = commitIndex
}

func (s *RaftLogImpl) GetLastLogIndex() uint64 {
	return s.lastLogIndex
}

func (s *RaftLogImpl) GetLastLogTerm() uint64 {
	return s.lastLogTerm
}

func (s *RaftLogImpl) GetLogTermByIndex(logIndex uint64) uint64 {
	entry := &pb.Entry{}

	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		k := strconv.FormatUint(logIndex, 10)
		v := bucket.Get([]byte(k))

		if err := proto.Unmarshal(v, entry); err != nil {
			log.Fatalf("parse entry failed: %v", err)
		}
		return nil
	})
	return entry.Term
}
