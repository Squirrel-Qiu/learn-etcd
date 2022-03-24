package gate

import (
	"context"
	"github.com/Squirrel-Qiu/learn-etcd/raft"
)

type gate struct {
	raft raft.Raft
}

func (g *gate) GetData(ctx context.Context, keyDatas *KeyData) (valueDatas *ValueData, err error) {
}

func (g *gate) DeleteData(ctx context.Context, keyDatas *KeyData) (deleteResult *DeleteResult, err error) {
}

func (g *gate) UpdateData(ctx context.Context, data *Data) (updateResult *UpdateResult, err error) {
	updateResult = &UpdateResult{Success: false}
	if g.raft.UpdateData(data.Key, data.Value) {
		updateResult.Success = true
	}
	return updateResult, nil
}
