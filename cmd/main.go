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

	time.Sleep(2 * time.Second)
	log.Println("--- state after 2 seconds ---")
	for _, id := range ids {
		n := nodes[id]
		log.Printf("node %d: state=%s term=%d votedFor=%d", id, n.State(), n.Term(), n.VotedFor())
	}
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
