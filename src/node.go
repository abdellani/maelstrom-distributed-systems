package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeWrapper struct {
	node  *maelstrom.Node
	id    string
	idMtx sync.RWMutex

	seqMtx sync.Mutex
	seqNum int

	bcMtx           sync.RWMutex
	receivedNumbers []int
}

func NewNode() *NodeWrapper {
	n := NodeWrapper{
		node:            maelstrom.NewNode(),
		receivedNumbers: []int{},
	}
	n.defineHandlers()
	return &n
}

func (n *NodeWrapper) defineHandlers() {
	n.node.Handle("init", n.init)
	n.node.Handle("generate", n.generate)
	n.node.Handle("broadcast", n.broadcast)
	n.node.Handle("read", n.read)
	n.node.Handle("topology", n.topology)
}

func (n *NodeWrapper) init(msg maelstrom.Message) error {
	var body struct {
		ID string `json:"node_id"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.setID(body.ID)
	responseBody := map[string]string{}
	responseBody["type"] = "init_ok"
	return n.node.Reply(msg, responseBody)
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
	id := fmt.Sprintf("%s_%d", n.getID(), n.seqNum)
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
func (n *NodeWrapper) setID(id string) {
	n.idMtx.Lock()
	defer n.idMtx.Unlock()
	n.id = id
}

func (n *NodeWrapper) getID() string {
	n.idMtx.RLock()
	defer n.idMtx.RLock()
	return n.id
}
