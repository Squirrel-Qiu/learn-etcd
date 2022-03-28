package storage

import (
	bolt "go.etcd.io/bbolt"
	"log"
)

type KVStorage interface {
	Get(key []byte) (value []byte)
	Add(key, value []byte)
	Delete(key []byte)
}

type KVImpl struct {
	db *bolt.DB
}

func NewKVStorage(db *bolt.DB) *KVImpl {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err != nil {
			log.Fatalf("get data-bucket failed: %v", err)
		}

		return nil
	})

	return &KVImpl{
		db: db,
	}
}

func (s KVImpl) Get(key []byte) (value []byte) {
	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("kv-data"))

		value = bucket.Get(key)
		return nil
	})

	return value
}

func (s KVImpl) Add(key, value []byte) {
	s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err != nil {
			log.Fatalf("get data-bucket failed: %v", err)
		}

		if err2 := bucket.Put(key, value); err2 != nil {
			log.Fatalf("put data into bucket failed: %v", err2)
		}

		return nil
	})
}

func (s KVImpl) Delete(key []byte) {
	s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("kv-data"))

		if err := bucket.Delete(key); err != nil {
			log.Fatalf("delete data failed: %v", err)
		}
		return nil
	})
}
