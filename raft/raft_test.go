package raft

import (
	"context"
	"strconv"
	"testing"
	"time"

	pb "github.com/Squirrel-Qiu/learn-etcd/proto"
	"google.golang.org/grpc"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	//h.CheckSingleLeader()
	//time.Sleep(3 * time.Second)
	leaderId, _ := h.CheckSingleLeader()

	addr := "127.0.0.1:4314" + strconv.FormatUint(leaderId, 10)
	client, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("client dial to raft-leader failed %v", err)
	}

	data1 := &pb.ClientData{
		Key:   []byte("squ"),
		Value: []byte("squirrel-qiu"),
	}
	_, err = pb.NewGateClient(client).UpdateData(context.Background(), data1)
	if err != nil {
		t.Fatalf("rpc-update call failed: %v", err)
	}

	//time.Sleep(200 * time.Millisecond)

	data2 := &pb.ClientData{
		Key:   []byte("machine"),
		Value: []byte("I'm a machine"),
	}
	_, err = pb.NewGateClient(client).UpdateData(context.Background(), data2)
	if err != nil {
		t.Fatalf("rpc-update call failed: %v", err)
	}
	t.Log("update ok")

	time.Sleep(900 * time.Millisecond)

	key1 := &pb.KeyData{Key: []byte("squ")}
	v1, err := pb.NewGateClient(client).GetData(context.Background(), key1)
	if err != nil {
		t.Fatalf("rpc-get call failed: %v", err)
	}
	t.Log("when key=squ, value=", string(v1.Value))

	time.Sleep(5 * time.Second)
}
