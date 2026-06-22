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
	receivedNumbers := []int{}
	var seqMtx sync.Mutex
	var BCMtx sync.RWMutex

	generateID := func() string {
		seqMtx.Lock()
		defer seqMtx.Unlock()
		seqNum++
		id := fmt.Sprintf("%d_%d", nodeId, seqNum)
		log.Printf("id: %s\n", id)
		return id
	}

	addNumToReceivedBC := func(msg int) {
		BCMtx.Lock()
		defer BCMtx.Unlock()
		receivedNumbers = append(receivedNumbers, msg)
	}

	getReceivedNumbers := func() *[]int {
		BCMtx.RLock()
		defer BCMtx.RUnlock()
		nums := make([]int, len(receivedNumbers))
		copy(nums, receivedNumbers)
		return &nums
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

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Type    string `json:"type"`
			Message int    `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		addNumToReceivedBC(body.Message)
		respBody := map[string]string{
			"type": "topology_ok",
		}

		respBody["type"] = "broadcast_ok"
		return n.Reply(msg, respBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		nums := getReceivedNumbers()
		body["type"] = "read_ok"
		body["messages"] = *nums
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		respBody := map[string]string{
			"type": "topology_ok",
		}
		return n.Reply(msg, respBody)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
