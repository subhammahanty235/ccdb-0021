package node

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

type Sender interface {
	SendRequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool)
	SendAppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool)
}

type Node struct {
	mu     sync.Mutex
	id     int
	peers  []int
	sender Sender

	currentTerm int
	votedFor    int
	logEntries  []LogEntry

	commitIndex int
	lastApplied int
	state       State

	nextIndex  map[int]int
	matchIndex map[int]int

	electionResetEvent time.Time
	kv                 map[string][]Version
	clock              int64
	stopCh             chan struct{}

	gcThreshold int64
}

func NewNode(id int, peers []int, sender Sender) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		sender:      sender,
		currentTerm: 0,
		votedFor:    -1,
		logEntries:  []LogEntry{{Term: 0, Index: 0}},
		state:       Follower,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		kv:          make(map[string][]Version),
		stopCh:      make(chan struct{}),
	}
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.state
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.currentTerm
}

func (n *Node) VotedFor() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.votedFor
}

func (n *Node) becomeFollowerLocked(term int) {
	log.Printf("[node %d] becoming Follower, term %d -> %d", n.id, n.currentTerm, term)
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.electionResetEvent = time.Now()

	go n.runElectionTimer()
}

func (n *Node) lastLogIndexAndTermLocked() (int, int) {
	last := n.logEntries[len(n.logEntries)-1]
	return last.Index, last.Term
}

// Start launches the node's background loop. For Step 1 this only proves
// the node is alive and sitting in Follower state — no elections yet.
func (n *Node) Start() {
	n.mu.Lock()
	log.Printf("[node %d] starting as %s, term %d", n.id, n.state, n.currentTerm)
	n.electionResetEvent = time.Now()
	n.mu.Unlock()

	go n.runElectionTimer()

}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) Kill() {
	n.mu.Lock()
	log.Printf("[node %d] *** KILLED (simulated crash) ***", n.id)
	n.mu.Unlock()
	close(n.stopCh)
}

func (n *Node) Submit(cmd interface{}) (index int, isLeader bool) {
	log.Printf("Submitting log")
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, false
	}

	if put, ok := cmd.(Put); ok {
		n.clock++
		put.Timestamp = n.clock
		cmd = put
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   len(n.logEntries),
		Command: cmd,
	}

	n.logEntries = append(n.logEntries, entry)
	log.Printf("[node %d] appended %v at index %d (not yet committed)", n.id, cmd, entry.Index)
	return entry.Index, true
}

func (n *Node) Get(key string, callTime int64) (string, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if callTime < n.gcThreshold {
		return "", false, fmt.Errorf("read timestamp %d is below GC threshold %d", callTime, n.gcThreshold)
	}
	versions, ok := n.kv[key]
	if !ok {
		return "", false, nil
	}

	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if v.Timestamp <= callTime {
			return v.Value, true, nil
		}
	}

	return "", false, nil
}

func (n *Node) GetLatest(key string) (string, bool, error) {
	return n.Get(key, math.MaxInt64)
}

// checks whether any new entry now has a majprity of relicas storing it,
func (n *Node) updateCommitedIndexLocked() {
	// fmt.Printf("Update commityed index locked running\n")
	for N := len(n.logEntries) - 1; N > n.commitIndex; N-- {
		if n.logEntries[N].Term != n.currentTerm {
			continue
		}

		count := 1
		for _, peerID := range n.peers {
			if n.matchIndex[peerID] >= N {
				count++
			}
		}
		// fmt.Printf("checking majority here %d , and lengh of peer is %d--------------->\n", count, len(n.peers))
		if count*2 > len(n.peers)+1 { // majority
			n.commitIndex = N
			n.applyCommitedLocked()
			// apply commited index
			break
		} else {

			fmt.Printf("<--------------- No majority found \n")
		}
	}
}

func (n *Node) applyCommitedLocked() {
	// fmt.Printf("apply commityed index locked running\n")
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.logEntries[n.lastApplied]
		switch cmd := entry.Command.(type) {
		case Put:
			n.kv[cmd.Key] = append(n.kv[cmd.Key], Version{Timestamp: cmd.Timestamp, Value: cmd.Value})
			log.Printf("[node %d] applied %v at index %d", n.id, cmd, n.lastApplied)
		case GC:
			n.runGCLocked(cmd.Threshold)
			log.Printf("[node %d] applied GC threshold=%d at index %d", n.id, cmd.Threshold, n.lastApplied)
		}
	}
}

func (n *Node) runGCLocked(threshold int64) {
	if threshold <= n.gcThreshold {
		return
	}

	n.gcThreshold = threshold
	for key, versions := range n.kv {
		keepFrom := 0
		for i, v := range versions {
			if v.Timestamp <= threshold {
				keepFrom = i
			}
		}

		if keepFrom > 0 {
			pruned := append([]Version{}, versions[keepFrom:]...)
			log.Printf("[node %d] GC key %q: %d versions -> %d", n.id, key, len(versions), len(pruned))
			n.kv[key] = pruned
		}
	}
}
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// rule 1 : if a candidate is campaining on an old term, it's instantly rejected
	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	// rule 2 : if the candidates term is newer than ours, we are behiend, step down to follwoer and adopt their term before evaluating vote request

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	lastlogIndex, lastlogTerm := n.lastLogIndexAndTermLocked()

	logIsUpTodate := args.LastLogTerm > lastlogTerm || (args.LastLogTerm == lastlogTerm && args.LastLogIndex >= lastlogIndex)

	canVote := n.votedFor == -1 || n.votedFor == args.CandidateID
	if canVote && logIsUpTodate {
		n.votedFor = args.CandidateID
		n.electionResetEvent = time.Now()
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.state == Candidate {
		n.state = Follower
	}

	n.electionResetEvent = time.Now()

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= len(n.logEntries) {
			return AppendEntriesReply{n.currentTerm, false}
		}
		if n.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			return AppendEntriesReply{n.currentTerm, false}
		}
	}

	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertIndex + i
		if idx < len(n.logEntries) {
			if n.logEntries[idx].Term != entry.Term {
				n.logEntries = append(n.logEntries[:idx], args.Entries[i:]...)
				break
			}
			continue
		}
		n.logEntries = append(n.logEntries, args.Entries[i:]...)
		break

	}

	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}

		n.applyCommitedLocked()
	}

	return AppendEntriesReply{n.currentTerm, true}
}
