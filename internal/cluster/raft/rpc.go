package raft

import "encoding/json"

// Raft RPC message types. The cluster package maps these to ClusterMsg
// Type values for wire transport, but the raft package itself is
// cluster-independent to avoid import cycles.
const (
	// MsgRaftRequestVote is sent by a candidate to solicit votes.
	MsgRaftRequestVote uint8 = 20
	// MsgRaftVoteResponse is sent to a candidate to grant or deny a vote.
	MsgRaftVoteResponse uint8 = 21
	// MsgRaftAppendEntries is sent by the leader to replicate log entries
	// and as heartbeats.
	MsgRaftAppendEntries uint8 = 22
	// MsgRaftAppendResponse is sent by a follower to acknowledge
	// AppendEntries.
	MsgRaftAppendResponse uint8 = 23
)

// RaftMessage is the internal message envelope used by the Raft node.
// It is distinct from cluster.ClusterMsg to avoid import cycles. The
// cluster package's dispatch loop converts between the two.
type RaftMessage struct {
	Type uint8
	From string
	Term uint64
	Body []byte
}

// RequestVoteArgs is the payload of a RequestVote RPC.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the payload of an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transporter is the transport interface used by the Raft node.
// It sends and receives RaftMessage values. The cluster package
// adapts cluster.Transporter to this interface to avoid import cycles.
type Transporter interface {
	SendRaft(addr string, msg *RaftMessage) error
}

// EncodeRequestVoteArgs encodes args to JSON bytes.
func EncodeRequestVoteArgs(args *RequestVoteArgs) []byte {
	return encodeJSON(args)
}

// DecodeRequestVoteArgs decodes JSON bytes into args.
func DecodeRequestVoteArgs(body []byte, args *RequestVoteArgs) error {
	return decodeJSON(body, args)
}

// EncodeRequestVoteReply encodes reply to JSON bytes.
func EncodeRequestVoteReply(reply *RequestVoteReply) []byte {
	return encodeJSON(reply)
}

// DecodeRequestVoteReply decodes JSON bytes into reply.
func DecodeRequestVoteReply(body []byte, reply *RequestVoteReply) error {
	return decodeJSON(body, reply)
}

// EncodeAppendEntriesArgs encodes args to JSON bytes.
func EncodeAppendEntriesArgs(args *AppendEntriesArgs) []byte {
	return encodeJSON(args)
}

// DecodeAppendEntriesArgs decodes JSON bytes into args.
func DecodeAppendEntriesArgs(body []byte, args *AppendEntriesArgs) error {
	return decodeJSON(body, args)
}

// EncodeAppendEntriesReply encodes reply to JSON bytes.
func EncodeAppendEntriesReply(reply *AppendEntriesReply) []byte {
	return encodeJSON(reply)
}

// DecodeAppendEntriesReply decodes JSON bytes into reply.
func DecodeAppendEntriesReply(body []byte, reply *AppendEntriesReply) error {
	return decodeJSON(body, reply)
}

func encodeJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func decodeJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
