package node

import "fmt"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	// Entries - > Enpty for heartbeat
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (a AppendEntriesArgs) String() string {
	kind := "AppendEntries"
	if len(a.Entries) == 0 {
		kind = "Heartbeat"
	}

	return fmt.Sprintf("%s{term=%d leader=%d prevIdx=%d entries=%d}", kind, a.Term, a.LeaderID, a.PrevLogIndex, len(a.Entries))
}

// SC
type Put struct {
	Key   string
	Value string
}
