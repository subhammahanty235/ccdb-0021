package node

import (
	"fmt"
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
				//  start election
				n.startElection()
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
	// TU - IMP
	for _, peerId := range n.peers {
		n.nextIndex[peerId] = len(n.logEntries)
		n.matchIndex[peerId] = 0
	}
	// run heartbeats
	go n.runHeartBeat()
}

func (n *Node) runHeartBeat() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		savedTerm := n.currentTerm
		n.mu.Unlock()

		for _, peerID := range n.peers {
			go func(peerId int) {
				n.mu.Lock()
				ni := n.nextIndex[peerId]
				fmt.Printf("peer id is %d and next index is %d\n", peerId, ni)
				fmt.Println("Log entries length is ", len(n.logEntries))
				prevLogIndex := ni - 1
				fmt.Println("Prev log index is  ", prevLogIndex)
				prevLogTerm := n.logEntries[prevLogIndex].Term
				entries := append([]LogEntry{}, n.logEntries[ni:]...)
				leadercommit := n.commitIndex
				n.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         savedTerm,
					LeaderID:     n.id,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leadercommit,
				}

				reply, ok := n.sender.SendAppendEntries(peerID, args)
				if !ok {
					return
				}

				n.mu.Lock()
				defer n.mu.Unlock()

				if n.state != Leader || n.currentTerm != savedTerm {
					return
				}

				if reply.Term > n.currentTerm {
					n.becomeFollowerLocked(reply.Term)
					return
				}

				if reply.Success {
					n.nextIndex[peerID] = ni + len(entries)
					n.updateCommitedIndexLocked()
				} else {
					if n.nextIndex[peerID] > 1 {
						n.nextIndex[peerID]--
					}
				}

			}(peerID)
		}

		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}
