package storage

import (
	bolt "go.etcd.io/bbolt"
)

type KVStorage interface {
	Get(key []byte) (value []byte, err error)
	Add(key, value []byte) error
	Delete(key []byte) error
}

type KVImpl struct {
	db *bolt.DB
}

func NewKVStorage(db *bolt.DB) *KVImpl {
	return &KVImpl{
		db: db,
	}
}

func (s KVImpl) Get(key []byte) (value []byte, err error) {
	if err = s.db.View(func(tx *bolt.Tx) error {
		bucket, err2 := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err2 != nil {
			return err2
		}
		value = bucket.Get(key)
		return nil
	}); err != nil {
		return nil, err
	}
	return value, nil
}

func (s KVImpl) Add(key, value []byte) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket, err2 := tx.CreateBucketIfNotExists([]byte("kv-data"))
		if err2 != nil {
			return err2
		}

		if err3 := bucket.Put(key, value); err3 != nil {
			return err3
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s KVImpl) Delete(key []byte) error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("kv-data"))

		if err2 := bucket.Delete(key); err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
