package node

import (
	"log"
	"math/rand"
	"time"
)

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) runElectionTimer() {
	timeoutDuration := electionTimeout()
	n.mu.Lock()
	termStarted := n.currentTerm
	n.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			// leaders dont run elections themselves
			if n.state == Leader {
				n.mu.Unlock()
				return
			}
			// term moved on since we started watching; a newer timer owns this now
			if termStarted != n.currentTerm {
				n.mu.Unlock()
				return
			}

			elapsed := time.Since(n.electionResetEvent)
			n.mu.Unlock()

			if elapsed >= timeoutDuration {
				// n.start - start election

				return
			}

		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	savedTerm := n.currentTerm
	n.votedFor = n.id //vote for self
	n.electionResetEvent = time.Now()
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTermLocked()
	log.Printf("[node %d] timed out, starting election for term %d", n.id, savedTerm)
	n.mu.Unlock()

	votes := 1

	for _, peerId := range n.peers {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:         savedTerm,
				CandidateID:  n.id,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			reply, ok := n.sender.SendRequestVote(peerId, args)
			if !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != savedTerm {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes++
				if votes*2 > len(n.peers)+1 {
					// become leader
					n.becomeLeaderLocked()
				}
			}
		}(peerId)
	}

	go n.runElectionTimer()

}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	log.Printf("[node %d] *** elected LEADER *** for term %d", n.id, n.currentTerm)
	// run heartbeats
}
