package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeWrapper struct {
	id   int
	node *maelstrom.Node

	seqMtx sync.Mutex
	seqNum int

	bcMtx           sync.RWMutex
	receivedNumbers []int
}

func NewNode() *NodeWrapper {
	n := NodeWrapper{
		id:              -1,
		node:            maelstrom.NewNode(),
		receivedNumbers: []int{},
	}
	n.DefineHandlers()
	return &n
}

func (n *NodeWrapper) DefineHandlers() {
	n.node.Handle("echo", n.echo)
	n.node.Handle("generate", n.generate)
	n.node.Handle("broadcast", n.broadcast)
	n.node.Handle("read", n.read)
	n.node.Handle("topology", n.topology)
}

func (n *NodeWrapper) echo(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return n.node.Reply(msg, body)
}

func (n *NodeWrapper) generate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "generate_ok"
	body["id"] = n.generateID()
	return n.node.Reply(msg, body)
}

func (n *NodeWrapper) broadcast(msg maelstrom.Message) error {
	var body struct {
		Type    string `json:"type"`
		Message int    `json:"message"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.addNumToReceivedBC(body.Message)
	respBody := map[string]string{
		"type": "topology_ok",
	}

	respBody["type"] = "broadcast_ok"
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) read(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	nums := n.getReceivedNumbers()
	body["type"] = "read_ok"
	body["messages"] = *nums
	return n.node.Reply(msg, body)
}

func (n *NodeWrapper) topology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	respBody := map[string]string{
		"type": "topology_ok",
	}
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) generateID() string {
	n.seqMtx.Lock()
	defer n.seqMtx.Unlock()
	n.seqNum++
	if n.id == -1 {
		n._setRandomID()
	}
	id := fmt.Sprintf("%d_%d", n.id, n.seqNum)
	return id

}

func (n *NodeWrapper) addNumToReceivedBC(msg int) {
	n.bcMtx.Lock()
	defer n.bcMtx.Unlock()
	n.receivedNumbers = append(n.receivedNumbers, msg)
}

func (n *NodeWrapper) getReceivedNumbers() *[]int {
	n.bcMtx.RLock()
	defer n.bcMtx.RUnlock()
	nums := make([]int, len(n.receivedNumbers))
	copy(nums, n.receivedNumbers)
	return &nums

}

// the calling method should hold the mutex
func (n *NodeWrapper) _setRandomID() {
	n.id = rand.IntN(100)
}
