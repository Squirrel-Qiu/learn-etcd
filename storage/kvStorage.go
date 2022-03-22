package storage

type kvStorage interface {
	Get(key []byte) (value []byte)
	Add(key []byte, value []byte)
	Update()
}
