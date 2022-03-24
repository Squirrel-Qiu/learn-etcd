package storage

import (
	bolt "go.etcd.io/bbolt"
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
		bucket, err2 := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err2 != nil {
			return err2
		}

		if err3 := bucket.Put(key, value); err3 != nil {
			return err3
		}
		return nil
	})
}

func (s KVImpl) Delete(key []byte) {
	s.db.Update(func(tx *bolt.Tx) error {
		bucket, err2 := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err2 != nil {
			return err2
		}

		if err3 := bucket.Delete(key); err3 != nil {
			return err3
		}
		return nil
	})
}
