// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	//随机化选举间隔
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

/*-------------------------------------初始化函数 START------------------------------------*/

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:  c.ID,
		Prs: make(map[uint64]*Progress),
	}
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	r.RaftLog = newLog(c.Storage)
	hardState, _, _ := c.Storage.InitialState() //读取hardstate中的term和vote
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.State = StateFollower
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = None
	r.PendingConfIndex = None
	r.votes = make(map[uint64]bool)
	r.msgs = nil //TestRawNodeRestartFromSnapshot2C 中，want 里的 msg 为 nil，即测试点预期 newRaft 处的 msg 应该为 nil，而不是 make 一个空切片。
	r.Lead = None
	return r
}

/*-------------------------------------初始化函数 END------------------------------------*/

/*-------------------------------------SEND 函数 START------------------------------------*/

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 向指定节点发送日志复制消息
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	entries := r.RaftLog.getEntries(r.Prs[to].Next, r.Prs[r.id].Next)

	// 将 []eraftpb.Entry 转换为 []*eraftpb.Entry
	var entryPtrs []*pb.Entry
	for i := range entries {
		entryPtrs = append(entryPtrs, &entries[i])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entryPtrs,
		//Reject:  false,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Commit:  r.RaftLog.committed,
	})

	r.heartbeatElapsed = 0
}

func (r *Raft) sendRequestVote(to uint64) {
	logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		LogTerm: logterm,
		Index:   r.RaftLog.LastIndex(),
	})
}

/*-------------------------------------SEND 函数 END------------------------------------*/

/*-------------------------------------工具 函数 START------------------------------------*/

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
			})
		}
	}
}

// resetElectionTimeout resets the election timeout to a random value
// between [electionTimeout, 2*electionTimeout).
func (r *Raft) randomizedElectionTimeout() int {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	return r.randomElectionTimeout
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			//发送选举消息不应该becomeCandidate()中实现
			if len(r.Prs) == 1 {
				// Single-node cluster, become leader immediately
				r.becomeLeader()
				return nil
			}
			for id := range r.Prs {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				// Single-node cluster, become leader immediately
				r.becomeLeader()
				return nil
			}
			for id := range r.Prs {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			return nil
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m)
		case pb.MessageType_MsgTimeoutNow:
		}
	}
	return nil
}

// 判断是否可以更新Commit索引
func (r *Raft) maybeCommit() bool {
	commitres := false
	matchIndexes := make([]uint64, 0, len(r.Prs))

	//将所有节点的match排序，取中位数，判断这个位置的日志的term和当前term是否一致，一致则更新
	for _, pr := range r.Prs {
		matchIndexes = append(matchIndexes, pr.Match)
	}
	sort.Slice(matchIndexes, func(i, j int) bool { return matchIndexes[i] < matchIndexes[j] })

	// 找到大多数节点的 Match 值
	majorityIndex := matchIndexes[(len(matchIndexes)-1)/2] //下标从零开始，所以需要减一，但不能是除以2之后减一（下标越界），而是总长度减一之后除以2
	// 检查 majorityIndex 对应的日志条目的任期是否与当前任期一致
	term, _ := r.RaftLog.Term(majorityIndex)
	if majorityIndex > r.RaftLog.committed && term == r.Term {
		r.RaftLog.committed = majorityIndex
		commitres = true
	}
	return commitres
}

/*-------------------------------------工具 函数 END------------------------------------*/

/*-------------------------------------节点状态改变 函数 START------------------------------------*/

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		//只有Term大于当前Term时才重制Vote
		r.Vote = None
	}

	r.Term = term
	r.votes = make(map[uint64]bool) //投票记录清零，下同
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
	r.randomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++      //增加当前任期
	r.Vote = r.id //投票给自己
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true          //投票给自己
	r.electionElapsed = 0         //重置选举计时器
	r.leadTransferee = None       //没有转移leader
	r.randomizedElectionTimeout() //随机化选举间隔
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id //设置leader为自己
	r.votes = make(map[uint64]bool)

	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 //更新所有节点的进度
		r.Prs[id].Match = 0
	}

	//追加noop entry
	//noop entry的term和index会在handlePropose()中设置
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

/*-------------------------------------节点状态改变 函数 END------------------------------------*/

/*-------------------------------------HANDLE 函数 START------------------------------------*/

func (r *Raft) handleTransferLeader(m pb.Message) {}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, m.From)
		return
	}

	//回复的commit小于当前节点的commit，说明当前节点的commit需要更新
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
		return
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}

	if m.Reject {
		// 如果跟随者拒绝，回退索引
		prevLogIndex := m.Index
		prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
		entries := r.RaftLog.getEntries(m.Index+1, r.Prs[r.id].Next)
		// 将 []eraftpb.Entry 转换为 []*eraftpb.Entry
		var entryPtrs []*pb.Entry
		for i := range entries {
			entryPtrs = append(entryPtrs, &entries[i])
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: entryPtrs,
			Commit:  r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
		return
	}

	// 更新跟随者的 Match 和 Next
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		// 如果跟随者的 Match 小于当前节点的日志索引，发送 Append 消息
		r.sendAppend(m.From)
	}
	// 检查是否可以提交
	// 如果当前任期和日志的任期相同，则可以判断是否提交
	currentLogTerm, _ := r.RaftLog.Term(m.Index)
	if r.Term == currentLogTerm {
		// 检查是否可以提交
		//commit索引改变才发消息
		if r.maybeCommit() {
			//同步Commit索引
			for id := range r.Prs {
				if id != r.id {
					//这里应该可以通过在sendAppend（）中添加判断，来优化代码
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgAppend,
						Term:    r.Term,
						From:    r.id,
						To:      id,
						//不带LogTerm和Index的话会默认设置为0，应当设置为消息的
						LogTerm: m.LogTerm,
						Index:   m.Index,
						Entries: nil,
						Commit:  r.RaftLog.committed,
					})
				}
			}
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	//更新日志索引
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.appendEntry(*entry)
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//这里是否需要对非leader节点的Next和Match进行更新？？
	//以及为何更新非leader节点时，非leader的Next节点只能更新到当前leader节点的lastindex？？而不是像becomeleader（）时那样更新为leader节点的lastindex+1？？
	// for id := range r.Prs {
	// 	if id != r.id {
	// 		r.Prs[id].Next = r.RaftLog.LastIndex()
	// 	} else {
	// 		r.Prs[id].Match = r.RaftLog.LastIndex()
	// 		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	// 	}
	// }

	// 如果是单节点集群，直接将 Match 值作为 committed
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
		return
	}

	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if m.Term < r.Term {
		return
	}
	if m.Reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}

	voteCount := 0
	voteNotCount := 0
	for _, v := range r.votes {
		if v {
			voteCount++
		}
		if !v {
			voteNotCount++
		}
	}
	if voteCount >= len(r.Prs)/2+1 { //大于半数节点投支持票
		r.becomeLeader()
		return
	}
	if voteNotCount >= len(r.Prs)/2+1 { //大于半数节点投反对票
		r.becomeFollower(r.Term, None)
		return
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	r.electionElapsed = 0

	logterm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	ridx := r.RaftLog.LastIndex()

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None) //如果更大任期的消息类型是 MsgRequestVote，领导者应为空。
	}

	//候选者任期更小
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	//候选者日志任期更小
	if m.LogTerm < logterm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	//候选者日志任期相同但日志索引更小
	if m.LogTerm == logterm && m.Index < ridx {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	//已经投过票
	if r.Vote != None && r.Vote != m.From {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}
	//r.becomeFollower(m.Term, m.From) 是因为不一定选举上，所以发生选举后r.Lead要保持None吗？？？
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Reject:  false,
	})
	r.Vote = m.From
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	//收到任期相等的append请求也要回退到follower
	r.becomeFollower(m.Term, m.From)

	currentLogTerm, _ := r.RaftLog.Term(m.Index)
	nextLogTerm, _ := r.RaftLog.Term(m.Index + 1)

	//先比对应索引日志上的任期
	if m.LogTerm != currentLogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Index:   r.RaftLog.committed, //回退到committed
		})
		return
	}

	returnIndex := m.Index + 1
	if m.Entries != nil && m.Entries[0].Term != nextLogTerm { //找到复制起点
		//日志裁剪
		r.RaftLog.entries = r.RaftLog.getEntries(uint64(r.RaftLog.dummyIndex), uint64(m.Index+1))
		//维护stabled日志
		r.RaftLog.stabled = min(r.RaftLog.stabled, m.Index)
		//追加日志
		for _, entry := range m.Entries {
			r.RaftLog.appendEntry(*entry)
		}
		returnIndex = r.RaftLog.LastIndex()
	}
	// 更新 committed 索引
	if m.Commit > r.RaftLog.committed {
		if m.Entries != nil {
			r.RaftLog.committed = min(m.Commit, returnIndex)
		} else {
			// 如果没有追加日志，直接更新 committed 索引
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Reject:  false,
		Index:   returnIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.heartbeatElapsed = 0

	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
			Commit:  r.RaftLog.committed,
		})
		return
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Commit:  r.RaftLog.committed,
	})
	r.Lead = m.From //在心跳里更新leader
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

/*-------------------------------------HANDLE 函数 END------------------------------------*/

/*-------------------------------------NODE变动 函数 START------------------------------------*/

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

/*-------------------------------------NODE变动 函数 END------------------------------------*/
