package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/akshaydubey05/GoRedis/internal/cluster"
	"github.com/akshaydubey05/GoRedis/internal/fsm"
	"github.com/akshaydubey05/GoRedis/internal/raft"
	"github.com/akshaydubey05/GoRedis/internal/store"
	"github.com/akshaydubey05/GoRedis/internal/transport"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	id := flag.Int("id", 0, "this node's numeric ID")
	clusterSpec := flag.String("cluster", "",
		"comma-separated node list: id=raftAddr@clientAddr,... "+
			"e.g. 0=127.0.0.1:7000@127.0.0.1:6380,1=127.0.0.1:7001@127.0.0.1:6381")
	dataDir := flag.String("data", "data", "directory to store this node's Raft state")
	flag.Parse()

	if *clusterSpec == "" {
		log.Fatal("--cluster is required, e.g. " +
			"0=127.0.0.1:7000@127.0.0.1:6380,1=127.0.0.1:7001@127.0.0.1:6381,2=127.0.0.1:7002@127.0.0.1:6382")
	}

	raftAddrs := map[int]string{}
	clientAddrs := map[int]string{}
	for _, part := range strings.Split(*clusterSpec, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			log.Fatalf("bad --cluster entry: %q", part)
		}
		peerID, err := strconv.Atoi(kv[0])
		must(err)
		addrs := strings.SplitN(kv[1], "@", 2)
		if len(addrs) != 2 {
			log.Fatalf("bad --cluster entry (need raftAddr@clientAddr): %q", part)
		}
		raftAddrs[peerID] = addrs[0]
		clientAddrs[peerID] = addrs[1]
	}

	var peers []int
	for peerID := range raftAddrs {
		if peerID != *id {
			peers = append(peers, peerID)
		}
	}

	must(os.MkdirAll(*dataDir, 0o755))

	applyCh := make(chan raft.ApplyMsg, 256)
	tr := transport.NewTCP(raftAddrs)
	storagePath := filepath.Join(*dataDir, "raft.state")
	rn := raft.NewNode(*id, peers, tr, raft.FileStorage{Path: storagePath}, applyCh)

	db := store.New()
	machine := fsm.New(db)
	node := cluster.New(rn, machine, applyCh, clientAddrs)

	must(transport.Serve(raftAddrs[*id], rn))
	log.Printf("node %d up — raft: %s, clients: %s", *id, raftAddrs[*id], clientAddrs[*id])

	must(node.ServeRESP(clientAddrs[*id])) // blocks forever
}