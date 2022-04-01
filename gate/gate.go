package gate

import (
	"context"
	"github.com/Squirrel-Qiu/learn-etcd/ch_request"
	pb "github.com/Squirrel-Qiu/learn-etcd/proto"
)

type Gate struct {
	getDataCh    chan ch_request.GetDataStruct
	deleteDataCh chan ch_request.DeleteDataStruct
	updateDataCh chan ch_request.UpdateDataStruct
}

func NewGate(getDataCh chan ch_request.GetDataStruct, deleteDataCh chan ch_request.DeleteDataStruct,
	updateDataCh chan ch_request.UpdateDataStruct) *Gate {
	return &Gate{
		getDataCh:    getDataCh,
		deleteDataCh: deleteDataCh,
		updateDataCh: updateDataCh,
	}
}

func (g *Gate) GetData(ctx context.Context, key *pb.KeyData) (value *pb.ValueData, err error) {
	getData := ch_request.GetDataStruct{
		Key:   key.Key,
		Value: make(chan []byte, 1),
	}

	g.getDataCh <- getData

	value = &pb.ValueData{Value: <-getData.Value}
	return value, nil
}

func (g *Gate) DeleteData(ctx context.Context, key *pb.KeyData) (deleteResult *pb.Result, err error) {
	deleteData := ch_request.DeleteDataStruct{
		Key:     key.Key,
		Success: make(chan bool, 1),
	}

	g.deleteDataCh <- deleteData

	deleteResult = &pb.Result{Success: <-deleteData.Success}
	return deleteResult, nil
}

func (g *Gate) UpdateData(ctx context.Context, data *pb.ClientData) (updateResult *pb.Result, err error) {
	updateData := ch_request.UpdateDataStruct{
		Data:    data,
		Success: make(chan bool, 1),
	}

	g.updateDataCh <- updateData

	updateResult = &pb.Result{Success: <-updateData.Success}
	return updateResult, nil
}
