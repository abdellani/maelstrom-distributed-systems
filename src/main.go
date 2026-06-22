package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	nodeId := rand.IntN(100)
	seqNum := 0
	var mtx sync.Mutex

	generateID := func() string {
		mtx.Lock()
		defer mtx.Unlock()
		seqNum++
		id := fmt.Sprintf("%d_%d", nodeId, seqNum)
		log.Printf("id: %s\n", id)
		return id
	}

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "echo_ok"
		return n.Reply(msg, body)

	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "generate_ok"
		body["id"] = generateID()
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
