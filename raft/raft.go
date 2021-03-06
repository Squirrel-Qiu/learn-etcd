package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Squirrel-Qiu/learn-etcd/ch_request"
	"github.com/Squirrel-Qiu/learn-etcd/proto"
	"github.com/Squirrel-Qiu/learn-etcd/storage"
	bolt "go.etcd.io/bbolt"
)

const DeLog = 1
const None uint64 = 0

type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	Dead
)

func (t StateType) String() string {
	switch t {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("illegal state")
	}
}

type raft struct {
	mu sync.Mutex

	id    uint64
	state StateType

	Term     uint64
	VotedFor uint64

	peerIds     []uint64
	peerClients map[uint64]proto.RPCommClient

	server *Server

	logSto storage.LogStorage
	kvSto  storage.KVStorage

	heartbeatTimeout time.Duration
	//electionResetEvent time.Time

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	voteReqCh  chan ch_request.VoteReqStruct
	entryReqCh chan ch_request.EntryStruct

	getDataCh    chan ch_request.GetDataStruct
	deleteDataCh chan ch_request.DeleteDataStruct
	updateDataCh chan ch_request.UpdateDataStruct
}

func initDB(id uint64) *bolt.DB {
	path := "../test0" + strconv.Itoa(int(id)) + ".db"
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout:        5 * time.Second,
		MmapFlags:      syscall.MAP_POPULATE,
		NoFreelistSync: true,
	})
	if err != nil {
		log.Fatalf("bolt open failed: %v", err)
	}
	return db
}

func newRaft(id uint64, peerIds []uint64, peerClients map[uint64]proto.RPCommClient, server *Server,
	ready <-chan struct{}, voteReqCh chan ch_request.VoteReqStruct, entryReqCh chan ch_request.EntryStruct,
	getDataCh chan ch_request.GetDataStruct, deleteDataCh chan ch_request.DeleteDataStruct,
	updateDataCh chan ch_request.UpdateDataStruct) *raft {
	db := initDB(id)
	r := &raft{
		id:               id,
		state:            StateFollower,
		peerIds:          peerIds,
		peerClients:      peerClients,
		VotedFor:         None,
		server:           server,
		logSto:           storage.NewRaftLog(db),
		kvSto:            storage.NewKVStorage(db),
		heartbeatTimeout: 100 * time.Millisecond,
		nextIndex:        make(map[uint64]uint64),
		matchIndex:       make(map[uint64]uint64),
		voteReqCh:        voteReqCh,
		entryReqCh:       entryReqCh,
		getDataCh:        getDataCh,
		deleteDataCh:     deleteDataCh,
		updateDataCh:     updateDataCh,
	}

	go func() {
		<-ready
		//r.mu.Lock()
		//r.electionResetEvent = time.Now()
		//r.mu.Unlock()
		r.runElectionTimeout()
	}()

	return r
}

func (r *raft) Report() (id uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.id, r.Term, r.state == StateLeader
}

func (r *raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = Dead
	r.dlog("becomes Dead")
}

func (r *raft) dlog(format string, args ...interface{}) {
	if DeLog == 1 {
		sprintf := fmt.Sprintf("[%d]", r.id) + format
		log.Printf(sprintf, args...)
	}
}

// ?????????Candidate???Follower?????????????????????
/*
	?????????????????????????????????
	1.????????????
	2.?????????????????????????????????????????????
	3.??????
*/
func (r *raft) runElectionTimeout() {
	r.mu.Lock()
	termStarted := r.Term
	r.mu.Unlock()
	r.dlog("?????????%d??????????????????", termStarted)

	for {
		var vq ch_request.VoteReqStruct
		var ae ch_request.EntryStruct

		select {
		case vq = <-r.voteReqCh:
			r.RequestVote(vq)
		case ae = <-r.entryReqCh:
			r.AppendEntries(ae)
		case <-time.After(time.Duration(500+rand.Intn(500)) * time.Millisecond):
			// ??????
			r.dlog("???????????????, ?????????????????????")

			r.startElection()
			return
		}
	}
}

// ??????candidate??????????????????
/*
	??????RequestVoteReply???????????????????????????????????????
	1.???????????????, ????????????
	2.????????????????????????Follower
	3.???????????????????????????
*/
func (r *raft) startElection() {
	r.state = StateCandidate
	r.Term += 1
	currentTerm := r.Term
	r.VotedFor = r.id
	r.dlog("??????????????? Term=%d , ????????????????????????--------------------------", currentTerm)
	//r.electionResetEvent = time.Now()

	lastLogIndex, lastLogTerm := r.logSto.GetLastLogIndex(), r.logSto.GetLastLogTerm()

	var wg sync.WaitGroup
	var resultMap sync.Map
	for _, id := range r.peerIds {
		wg.Add(1)
		go func(peerId uint64) {
			defer wg.Done()

			args := &proto.RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			r.dlog("???????????? %d ??????RequestVote??????: %+v", peerId, args)

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
			defer cancelFunc()

			if reply, err := r.peerClients[peerId].RequestVote(ctx, args); err == nil {
				r.dlog("?????? %d RequestVoteReply?????????", peerId)

				resultMap.Store(peerId, reply)
			}
		}(id)
	}

	wg.Wait()

	voteRequestResult := make([]*proto.RequestVoteReply, 0, len(r.peerIds))
	resultMap.Range(func(_, replyRaw any) bool {
		voteRequestResult = append(voteRequestResult, replyRaw.(*proto.RequestVoteReply))

		return true
	})

	var votesReceived uint64 = 1
	for _, v := range voteRequestResult {
		if v.Term > currentTerm {
			r.dlog("??????RequestVoteReply???????????????????????????")
			r.becomeFollower(v.Term)
			go r.runElectionTimeout()
			return
		} else if v.VoteGranted {
			votesReceived++
			if votesReceived > uint64(len(r.peerIds)+1)/2 {
				r.dlog("??????Leader===================================================")
				go r.becomeLeader()
				return
			}
		}
	}

	// ???peerIds?????????RequestVote???, ?????????????????? Candidate ????????????????????????
	if r.state == StateCandidate {
		r.dlog("????????????, ??????????????????????????????")
		time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)
		go r.runElectionTimeout()
	}
}

func (r *raft) becomeFollower(term uint64) {
	r.state = StateFollower
	r.Term = term
	r.VotedFor = None
}

func (r *raft) becomeLeader() {
	r.state = StateLeader

	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = r.logSto.GetCommitIndex() + 1
	}
	r.dlog("??????Leader, term=%d, nextIndex=%v, matchIndex=%v", r.Term, r.nextIndex, r.matchIndex)

	go func() {
		for {
			var getData ch_request.GetDataStruct
			var deleteData ch_request.DeleteDataStruct
			var updateData ch_request.UpdateDataStruct

			var isGet bool
			select {
			case getData = <-r.getDataCh:
				isGet = true
				r.GetData(getData)
			case deleteData = <-r.deleteDataCh:
				r.DeleteData(deleteData)
			case updateData = <-r.updateDataCh:
				r.UpdateData(updateData)
			case <-time.After(r.heartbeatTimeout):
			}

			if isGet {
				time.Sleep(r.heartbeatTimeout)
			}

			r.sendHeartbeats()

			r.mu.Lock()
			if r.state != StateLeader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

// Leader????????????
func (r *raft) sendHeartbeats() {
	r.mu.Lock()
	currentTerm := r.Term
	r.mu.Unlock()

	var wg sync.WaitGroup
	var resultMap sync.Map

	leaderCommit := r.logSto.GetCommitIndex()

	for _, id := range r.peerIds {
		wg.Add(1)
		go func(peerId uint64) {
			defer wg.Done()

			var entries []*proto.Entry
			var preLogTerm uint64

			x := r.nextIndex[peerId]
			entries = r.logSto.GetEntriesFromIndex(x)

			preLogIndex := x - 1
			preLogTerm = r.logSto.GetLogTermByIndex(preLogIndex)
			//if preLogIndex > 0 && preLogIndex <= r.logSto.GetLastLogIndex() {
			//} else if preLogIndex == 0 && r.logSto.GetLastLogIndex() != 0 {
			//	//preLogTerm = logs[preLogIndex].Term = 0
			//}

			args := &proto.AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     r.id,
				PreLogIndex:  preLogIndex,
				PreLogTerm:   preLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			r.dlog("???????????? %d ??????AppendEntries??????, Leader???????????????????????? %d", peerId, r.logSto.GetLastLogIndex())

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
			defer cancelFunc()

			reply, err := r.peerClients[peerId].AppendEntries(ctx, args)
			if err != nil {
				r.dlog("rpc error: %+v", err)
			} else {
				r.dlog("??????[%d]AppendEntriesReply?????????: %d", peerId, reply.LogIndex)
				resultMap.Store(peerId, reply)

			}
		}(id)
	}

	wg.Wait()

	appendResult := make(map[uint64]*proto.AppendEntriesReply, len(r.peerIds))
	resultMap.Range(func(peerId, replyRaw any) bool {
		appendResult[peerId.(uint64)] = replyRaw.(*proto.AppendEntriesReply)

		return true
	})

	for peerId, reply := range appendResult {
		if reply.Term > currentTerm {
			r.dlog("??????AppendEntriesReply???????????????????????????")
			r.becomeFollower(reply.Term)
			go r.runElectionTimeout()
			return
		}

		if r.state == StateLeader && currentTerm == reply.Term {
			if reply.Success {
				r.nextIndex[peerId] = reply.LogIndex + 1
				r.matchIndex[peerId] = r.nextIndex[peerId] - 1
				r.dlog("??????[%d]AppendEntries?????????????????????: nextIndex= %v, matchIndex= %v", peerId, r.nextIndex[peerId], r.matchIndex[peerId])

				// ?????????????????????????????????
				for i := leaderCommit + 1; i <= r.logSto.GetLastLogIndex(); i++ {
					// ????????????????????????????????????
					if r.logSto.GetLogTermByIndex(leaderCommit) == currentTerm {
						matchCount := 1 // Leader?????????
						for _, p := range r.peerIds {
							if r.matchIndex[p] >= i {
								matchCount++
							}
						}
						if matchCount > (len(r.peerIds)+1)/2 {
							r.logSto.SetCommitIndex(i)
						}
					}
				}

				// if ok commit
				if r.logSto.GetCommitIndex() != leaderCommit {
					r.dlog("????????????, Leader???commitIndex?????? %d", r.logSto.GetCommitIndex())
				}
			} else {
				r.nextIndex[peerId] -= 1
				r.dlog("?????? %d AppendEntries?????????????????????: nextIndex= %v", peerId, r.nextIndex[peerId])
			}
		}
	}
}

func (r *raft) RequestVote(vq ch_request.VoteReqStruct) {
	if r.state == Dead {
		return
	}
	r.dlog("??????RequestVote??????: %+v", vq.Args)

	if vq.Args.Term > r.Term {
		r.dlog("??????????????????RequestVoteArgs?????????, ??????Follower")
		r.becomeFollower(vq.Args.Term)
	}

	lastLogIndex, lastLogTerm := r.logSto.GetLastLogIndex(), r.logSto.GetLastLogTerm()

	reply := &proto.RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}

	// ????????????????????????????????????????????????Candidate
	if vq.Args.Term == r.Term &&
		(r.VotedFor == None || r.VotedFor == vq.Args.CandidateId) &&
		(vq.Args.LastLogTerm > lastLogTerm ||
			(vq.Args.LastLogTerm == lastLogTerm && vq.Args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.VotedFor = vq.Args.CandidateId
		//r.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = r.Term
	r.dlog("RequestVote??????: %+v", reply)

	vq.VoteRespCh <- reply
}

func (r *raft) AppendEntries(ae ch_request.EntryStruct) {
	if r.state == Dead {
		return
	}
	r.dlog("??????AppendEntries??????: %+v", ae.Args)

	if ae.Args.Term > r.Term {
		r.dlog("??????????????????AppendEntriesArgs?????????, ??????Follower")
		r.becomeFollower(ae.Args.Term)
	}

	reply := &proto.AppendEntriesReply{
		Term:     0,
		Success:  false,
		LogIndex: ae.Args.PreLogIndex,
	}
	r.dlog("preLogIndex is %d", reply.LogIndex)

	if ae.Args.Term == r.Term {
		// ???Leader?????????, ???????????????Follower (Leader??????)
		if r.state != StateFollower {
			r.becomeFollower(ae.Args.Term)
		}

		// ???????????????????????????-PreLogIndex?????????????????????PreLogTerm??????
		if len(ae.Args.Entries) == 0 {
			reply.Success = true
			r.dlog("?????????????????????, ??????????????????")
		} else if ae.Args.PreLogIndex == 0 || ae.Args.PreLogIndex <= r.logSto.GetLastLogIndex() &&
			ae.Args.PreLogTerm == r.logSto.GetLogTermByIndex(ae.Args.PreLogIndex) {
			reply.Success = true

			// ???????????????
			logInsertPosition := ae.Args.PreLogIndex
			newEntriesIndex := 0

			// ???log??????????????????, ???????????????
			for {
				if logInsertPosition >= r.logSto.GetLastLogIndex() || newEntriesIndex >= len(ae.Args.Entries) {
					break
				}
				if r.logSto.GetLogTermByIndex(logInsertPosition) != ae.Args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertPosition++
				newEntriesIndex++
			}

			if newEntriesIndex < len(ae.Args.Entries) {
				r.dlog("????????? %d ??????????????????????????? %v", logInsertPosition, ae.Args.Entries[newEntriesIndex:])

				r.logSto.AppendEntriesFromIndex(logInsertPosition, ae.Args.Entries[newEntriesIndex:])

				reply.LogIndex = r.logSto.GetLastLogIndex()
			}
		}

		if ae.Args.LeaderCommit > r.logSto.GetCommitIndex() {
			newLogs := r.logSto.GetAllEntries()
			ci := minIndex(ae.Args.LeaderCommit, r.logSto.GetLastLogIndex())

			r.dlog("???committed???????????????????????????????????? data-bucket")
			for _, entry := range newLogs[r.logSto.GetCommitIndex():ci] {
				switch entry.Type {
				case proto.MsgType_MsgUpdate:
					r.kvSto.Add(entry.Data.Key, entry.Data.Value)
				case proto.MsgType_MsgDelete:
					r.kvSto.Delete(entry.Data.Key)
				}
			}

			r.logSto.SetCommitIndex(ci)
			r.dlog("commitIndex?????? %d", ci)
		}
	}

	reply.Term = r.Term
	r.dlog("AppendEntries??????: %+v", reply)

	ae.EntryRespCh <- reply
}

func minIndex(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func (r *raft) GetData(getData ch_request.GetDataStruct) {
	getData.Value <- r.kvSto.Get(getData.Key)
}

func (r *raft) UpdateData(updateData ch_request.UpdateDataStruct) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k, v := updateData.Data.Key, updateData.Data.Value
	r.dlog("??????????????????{Key:%s, Value:%s}", k, v)
	if r.state == StateLeader {
		// update entry into logStorage before kvStorage
		data := &proto.Data{Key: k, Value: v}
		r.logSto.AppendEntry(&proto.Entry{
			Term: r.Term,
			Type: proto.MsgType_MsgUpdate,
			Data: data,
		})

		r.kvSto.Add(k, v)
	}

	updateData.Success <- true
}

func (r *raft) DeleteData(deleteData ch_request.DeleteDataStruct) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := deleteData.Key
	r.dlog("??????????????????")
	if r.state == StateLeader {
		// update entry into logStorage before kvStorage
		data := &proto.Data{Key: k}
		r.logSto.AppendEntry(&proto.Entry{
			Term: r.Term,
			Type: proto.MsgType_MsgDelete,
			Data: data,
		})

		r.kvSto.Delete(k)
	}

	deleteData.Success <- true
}
