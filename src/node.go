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

	neighborsMtx sync.Mutex
	neighbors    []string

	broadcastMtx        sync.RWMutex
	processedBroadCasts map[string]bool
}

func NewNode() *NodeWrapper {
	n := NodeWrapper{
		node:                maelstrom.NewNode(),
		receivedNumbers:     []int{},
		neighbors:           []string{},
		processedBroadCasts: map[string]bool{},
	}
	n.defineHandlers()
	return &n
}

func (n *NodeWrapper) defineHandlers() {
	n.node.Handle("init", n.init)
	n.node.Handle("generate", n.generate)
	n.node.Handle("broadcast", n.broadcast)
	n.node.Handle("broadcast_ok", n.broadcastOk)
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

type boardcastBody struct {
	Type        string `json:"type"`
	Message     int    `json:"message"`
	BoardcastID string `json:"braodcast_id"`
}

func (n *NodeWrapper) broadcast(msg maelstrom.Message) error {
	n.neighborsMtx.Lock()
	defer n.neighborsMtx.Unlock()
	body := boardcastBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	id := body.BoardcastID
	if id == "" || !n.isBoardcastIDAlreadyProcessed(id) {
		n.addNumToReceivedBC(body.Message)
		go n.propagateToNeighbors(body)
		if id != "" {
			n.markProcessBoardcastID(id)
		}
	}
	respBody := map[string]string{
		"type": "broadcast_ok",
	}
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) broadcastOk(_msg maelstrom.Message) error {
	return nil
}

func (n *NodeWrapper) propagateToNeighbors(msg boardcastBody) {
	neighbors := n.getNeighbors()
	if msg.BoardcastID == "" {
		id := n.generateID()
		msg.BoardcastID = id
	}
	for _, neighbor := range neighbors {
		n.node.Send(neighbor, msg)
	}
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
	n.neighborsMtx.Lock()
	defer n.neighborsMtx.Unlock()
	var body struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	neighbors := body.Topology[n.getID()]
	n.setNeighbors(neighbors)
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

func (n *NodeWrapper) setNeighbors(neighbors []string) {
	n.neighbors = neighbors
}

func (n *NodeWrapper) getNeighbors() []string {
	neighbors := make([]string, len(n.neighbors))
	copy(neighbors, n.neighbors)
	return neighbors
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
	defer n.idMtx.RUnlock()
	return n.id
}

func (n *NodeWrapper) markProcessBoardcastID(id string) {
	n.processedBroadCasts[id] = true
}
func (n *NodeWrapper) isBoardcastIDAlreadyProcessed(id string) bool {
	_, ok := n.processedBroadCasts[id]
	return ok
}
