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

	time.Sleep(500 * time.Millisecond)

	leaderID := findLeader(nodes, ids)
	if leaderID == 0 {
		log.Println("no leader elected yet, waiting a bit more...")
		time.Sleep(500 * time.Millisecond)
		leaderID = findLeader(nodes, ids)
	}
	// log.Printf("=== current leader is node %d — killing it now ===", leaderID)
	log.Printf("=== leader is node %d — submitting writes ===", leaderID)
	leader := nodes[leaderID]
	log.Printf("Submittitiiiiigggggg--------------------->")
	leader.Submit(node.Put{Key: "foo", Value: "bar"})
	// leader.Submit(node.Put{Key: "name", Value: "lundy"})
	leader.Submit(node.Put{Key: "foo", Value: "bar- v2"})
	leader.Submit(node.Put{Key: "foo", Value: "bar- v3"})
	leader.Submit(node.Put{Key: "foo", Value: "bar- v4"})
	leader.Submit(node.Put{Key: "foo", Value: "bar- v5"})

	log.Println("--- waiting for replication----")
	time.Sleep(500 * time.Millisecond)

	log.Println("---- reading foo from every node ----- ")
	// for _, id := range ids {
	// 	// if id == leaderID {
	// 	// 	log.Printf("node killed : %d", id)
	// 	// 	continue
	// 	// }
	// 	v, ok := nodes[id].GetLatest("foo")
	// 	log.Printf("node %d: foo=%q found=%v", id, v, ok)
	// }
	log.Println("---- time travel: before GC ----")
	for ts := int64(1); ts <= 4; ts++ {
		v, ok, err := leader.Get("foo", ts)
		log.Printf("foo as of t=%d: %q found=%v, err=%v", ts, v, ok, err)
	}

	log.Println("=== running GC with threshold=3 ===")
	leader.Submit(node.GC{Threshold: 3})
	time.Sleep(500 * time.Millisecond)

	log.Println("--- time travel AFTER gc ---")
	for ts := int64(1); ts <= 4; ts++ {
		v, ok, err := leader.Get("foo", ts)
		log.Printf("  foo as of t=%d: %q found=%v err=%v", ts, v, ok, err)
	}

	log.Println("--- latest read from every node (should still work) ---")
	for _, id := range ids {
		v, ok, err := nodes[id].GetLatest("foo")
		log.Printf("node %d: foo=%q found=%v err=%v", id, v, ok, err)
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
