package gate

import (
	"context"
	"github.com/Squirrel-Qiu/learn-etcd/raft"
)

type gate struct {
	raft raft.Raft
}

func (g *gate) GetData(ctx context.Context, key *KeyData) (value *ValueData, err error) {
	v, err := g.raft.GetData(key.Key)
	if err != nil {
		return nil, err
	}
	return &ValueData{Value: v}, nil
}

func (g *gate) DeleteData(ctx context.Context, key *KeyData) (deleteResult *Result, err error) {
	deleteResult = &Result{Success: true}
	if err = g.raft.DeleteData(key.Key); err != nil {
		deleteResult.Success = false
		return deleteResult, err
	}
	return deleteResult, nil
}

func (g *gate) UpdateData(ctx context.Context, data *Data) (updateResult *Result, err error) {
	updateResult = &Result{Success: true}
	if err = g.raft.UpdateData(data.Key, data.Value); err != nil {
		updateResult.Success = false
		return updateResult, err
	}
	return updateResult, nil
}
