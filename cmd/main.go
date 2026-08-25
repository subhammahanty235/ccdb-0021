package main

import (
	"ccdb/node"
	"ccdb/transport"
	"log"
	"time"
)

func main() {
	ids := []int{1, 2, 3}
	tr := transport.NewInMemoryTransport()

	nodes := make(map[int]*node.Node)

	for _, id := range ids {
		peers := otherIds(ids, id)
		n := node.NewNode(id, peers, tr)
		nodes[id] = n
		tr.Register(id, n)
	}

	for _, n := range nodes {
		n.Start()
	}

	log.Println("--- all 3 replicas up, sitting in Follower state ---")
	log.Println("--- watching for an election ---")

	// time.Sleep(500 * time.Microsecond)

	// leaderID := findLeader(nodes, ids)
	// if leaderID == 0 {
	// 	log.Println("no leader elected yet, waiting a bit more...")
	// 	time.Sleep(500 * time.Millisecond)
	// 	leaderID = findLeader(nodes, ids)
	// }
	// log.Printf("=== current leader is node %d — killing it now ===", leaderID)

	// nodes[leaderID].Kill()
	// tr.UnRegister(leaderID)

	log.Println("--- watching remaining nodes for re-election ---")
	time.Sleep(1 * time.Second)

	log.Println("---- final state ----- ")
	for _, id := range ids {
		// if id == leaderID {
		// 	log.Printf("node killed : %d", id)
		// 	continue
		// }
		n := nodes[id]
		log.Printf("node %d: state=%s term=%d votedFor=%d", id, n.State(), n.Term(), n.VotedFor())
	}
}

func findLeader(nodes map[int]*node.Node, ids []int) int {
	for _, id := range ids {
		if nodes[id].State() == node.Leader {
			return id
		}
	}

	return 0
}

func otherIds(all []int, selfid int) []int {
	var out []int
	for _, id := range all {
		if id != selfid {
			out = append(out, id)
		}
	}
	return out
}
