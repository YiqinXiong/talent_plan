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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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
	electionTimeout     int
	randElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		//heartbeatElapsed: 0,
		//electionElapsed:  0,
	}
	// 填补Prs
	lastIdx := r.RaftLog.LastIndex()
	firstIdx, _ := r.RaftLog.storage.FirstIndex()
	var match uint64
	for _, eachPeer := range c.peers {
		match = firstIdx - 1
		if eachPeer == r.id {
			match = lastIdx
		}
		r.Prs[eachPeer] =
			&Progress{Next: lastIdx + 1, Match: match}
	}
	// newRaft成为Follower
	r.becomeFollower(0, None)
	hs, _, _ := r.RaftLog.storage.InitialState()
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term, r.Vote, r.RaftLog.committed = hs.GetTerm(), hs.GetVote(), hs.GetCommit()
	return r
}

// sendMsg发送消息到msgs
func (r *Raft) sendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// append一个entry
func (r *Raft) appendEntries(entry pb.Entry) {
	r.RaftLog.entries = append(r.RaftLog.entries, entry)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 参照paper的P7
	// 领导人任期号
	term := r.Term
	// 领导人的Id，便于跟随者重定向请求
	leaderId := r.id
	// 发送心跳RPC
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    leaderId,
		Term:    term,
	})
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	//参照paper的P8
	// 候选人任期号
	term := r.Term
	// 候选人的Id，
	candidateId := r.id
	// 候选人的最后日志条目的索引值
	lastLogIndex := r.RaftLog.LastIndex()
	// 候选人的最后日志条目的任期号
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	// 发送请求投票RPC
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    candidateId,
		Term:    term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 发送MessageType_MsgBeat确认leader
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randElectionTimeout {
			r.electionElapsed = 0
			// 发送MessageType_MsgHup成为候选人并开始选举
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 重置超时计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 任期号
	r.Term = term
	// 未投票
	r.Vote = None
	// 转换状态
	r.State = StateFollower
	// 设置lead
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 重置超时计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 开始新任期
	r.Term++
	// 投票给自己
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// 转换状态
	r.State = StateCandidate
	// 设置lead
	r.Lead = None
	// 重设electionTimeout，范围在1到2倍的electionTimeout之间
	r.heartbeatElapsed = 0
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)

	// 由报错可知，peer的数量等于1，只能成为leader
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 重置超时计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 转换状态
	r.State = StateLeader
	// 设置lead
	r.Lead = r.id
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 转变为Candidate，开始选举
		r.becomeCandidate()
		// 广播请求投票的RPC
		for eachPeer := range r.Prs {
			if eachPeer != r.id {
				r.sendRequestVote(eachPeer)
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 转变为Candidate，开始选举
		r.becomeCandidate()
		// 广播请求投票的RPC
		for eachPeer := range r.Prs {
			if eachPeer != r.id {
				r.sendRequestVote(eachPeer)
			}
		}
	case pb.MessageType_MsgAppend:
		// 如果收到新的Leader的附加日志RPC，转变为Follower
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term >= r.Term {
			// 未reject的视作投票
			r.votes[m.From] = !m.Reject
			// 统计票数
			voteCount := 0
			for _, eachVote := range r.votes {
				if eachVote {
					voteCount++
				}
			}
			if voteCount > len(r.Prs)/2 {
				// 投票超过半数成为leader
				r.becomeLeader()
			} else if len(r.votes)-voteCount > len(r.Prs)/2 {
				// 未投票超过半数成为follower
				r.becomeFollower(r.Term, None)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		// 如果收到新的Leader的附加日志RPC，转变为Follower
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for eachPeer := range r.Prs {
			if eachPeer == r.id {
				continue
			}
			r.sendHeartbeat(eachPeer)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 收到term更大的消息，转为follower
	if m.Term > r.Term {
		//log.Infof(
		//	"Node %d, Term %d [m.Term(%d) > r.Term(%d)]",
		//	r.id, r.Term, m.Term, r.Term)
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}


// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	switch {
	case m.Term < r.Term:
		// 不处理term较小的消息
		fallthrough
	case r.Vote != m.From && r.Vote != None:
		// 已经投票给其他candidate，不处理
		fallthrough
	case lastLogTerm > m.LogTerm:
		//当前节点任期号更大，不处理
		fallthrough
	case lastLogTerm == m.LogTerm && lastLogIndex > m.Index:
		//相同任期号，当前节点最后条目索引值更大，不处理
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	default:
		// 投票
		r.Vote = m.From
		r.electionElapsed = 0
		r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  false,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch {
	case m.Term < r.Term:
		// 不处理term较小的消息
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	default:
		// 确认心跳来源，认定leader
		r.Lead = m.From
		r.electionElapsed = 0
		r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  false,
		})
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
