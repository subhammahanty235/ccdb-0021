package node

import (
	"log"
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
	stopCh             chan struct{}
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

		stopCh: make(chan struct{}),
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
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}
