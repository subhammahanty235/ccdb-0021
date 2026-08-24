package node

import (
	"log"
	"sync"
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

	stopCh chan struct{}
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

// Start launches the node's background loop. For Step 1 this only proves
// the node is alive and sitting in Follower state — no elections yet.
func (n *Node) Start() {
	log.Printf("[node %d] starting as %s, term %d", n.id, n.state, n.currentTerm)
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	return AppendEntriesReply{Term: n.currentTerm, Success: false}
}
