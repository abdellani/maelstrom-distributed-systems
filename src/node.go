package main

import (
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

	rnumsMtx        sync.RWMutex
	receivedNumbers []int

	broadcastMtx    sync.RWMutex
	BroadcastHander *BroadcastHandler
}

func NewNode() *NodeWrapper {
	n := NodeWrapper{
		node:            maelstrom.NewNode(),
		receivedNumbers: []int{},
	}
	n.BroadcastHander = NewBroadcastHandler(&n)
	n.defineHandlers()
	return &n
}

func (n *NodeWrapper) defineHandlers() {
	n.node.Handle("init", n.init)
	n.node.Handle("generate", n.generate)
	n.node.Handle("broadcast", n.broadcast)
	n.node.Handle("broadcast_btwn_nodes", n.broadcastBetweenNodes)
	n.node.Handle("broadcast_ok", n.broadcastOk)
	n.node.Handle("read", n.read)
	n.node.Handle("topology", n.topology)
}

type boardcastBody struct {
	Type        string `json:"type"`
	Message     int    `json:"message"`
	BoardcastID string `json:"braodcast_id"`
}

func (n *NodeWrapper) send(dest string, msg boardcastBody) {
	n.node.Send(dest, msg)
}

func (n *NodeWrapper) generateID() string {
	n.seqMtx.Lock()
	defer n.seqMtx.Unlock()
	n.seqNum++
	id := fmt.Sprintf("%s_%d", n.getID(), n.seqNum)
	return id

}

func (n *NodeWrapper) addNumToReceivedBC(msg int) {
	n.rnumsMtx.Lock()
	defer n.rnumsMtx.Unlock()
	n.receivedNumbers = append(n.receivedNumbers, msg)
}

func (n *NodeWrapper) getReceivedNumbers() *[]int {
	n.rnumsMtx.RLock()
	defer n.rnumsMtx.RUnlock()
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
