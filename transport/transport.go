package transport

import (
	"ccdb/node"
	"math/rand"
	"sync"
	"time"
)

type RPCHandler interface {
	HandleRequestVote(args node.RequestVoteArgs) node.RequestVoteReply
	HandleAppendEntries(args node.AppendEntriesArgs) node.AppendEntriesReply
}

type Transport interface {
	Register(id int, handler RPCHandler)
	SendRequestVote(to int, args node.RequestVoteArgs) (node.RequestVoteReply, bool)
	SendAppendEntries(to int, args node.AppendEntriesArgs) (node.AppendEntriesReply, bool)
}

type InMemoryTransport struct {
	mu      sync.RWMutex
	handers map[int]RPCHandler

	// to simlate network delay per rpc
	MinLatency time.Duration
	MaxLatency time.Duration

	DropRate float64
}

func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{
		handers:    make(map[int]RPCHandler),
		MinLatency: 2 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		DropRate:   0,
	}
}

func (t *InMemoryTransport) Register(id int, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handers[id] = handler
}

func (t *InMemoryTransport) UnRegister(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.handers, id)
}

func (t *InMemoryTransport) simulateNetwork() bool {
	if t.DropRate > 0 && rand.Float64() < t.DropRate {
		return false
	}

	delay := t.MinLatency
	if t.MaxLatency > t.MaxLatency {
		delay += time.Duration(rand.Int63n(int64(t.MaxLatency - t.MinLatency)))
	}

	time.Sleep(delay)
	return true
}

func (t *InMemoryTransport) SendRequestVote(to int, args node.RequestVoteArgs) (node.RequestVoteReply, bool) {
	t.mu.RLock()
	h, ok := t.handers[to]
	t.mu.RUnlock()

	if !ok {
		return node.RequestVoteReply{}, false
	}

	if !t.simulateNetwork() {
		return node.RequestVoteReply{}, false
	}

	return h.HandleRequestVote(args), true

}

func (t *InMemoryTransport) SendAppendEntries(to int, args node.AppendEntriesArgs) (node.AppendEntriesReply, bool) {
	t.mu.RLock()
	h, ok := t.handers[to]
	t.mu.RUnlock()
	if !ok {
		return node.AppendEntriesReply{}, false
	}
	if !t.simulateNetwork() {
		return node.AppendEntriesReply{}, false
	}
	return h.HandleAppendEntries(args), true
}
