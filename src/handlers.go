package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

func (n *NodeWrapper) generate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "generate_ok"
	body["id"] = n.generateID()
	return n.node.Reply(msg, body)
}

func (n *NodeWrapper) echo(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return n.node.Reply(msg, body)
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
	n.broadcastMtx.Lock()
	defer n.broadcastMtx.Unlock()
	var body struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	neighbors := body.Topology[n.getID()]
	n.BroadcastHander.setNeighbors(neighbors)
	respBody := map[string]string{
		"type": "topology_ok",
	}
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) broadcast(msg maelstrom.Message) error {
	n.broadcastMtx.Lock()
	defer n.broadcastMtx.Unlock()
	body := boardcastBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	newID := n.generateID()
	body.BoardcastID = newID
	body.Type = "broadcast_btwn_nodes"
	n.BroadcastHander.addBroadcastId(body.BoardcastID, &body)
	n.addNumToReceivedBC(body.Message)

	respBody := map[string]string{
		"type": "broadcast_ok",
	}
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) broadcastBetweenNodes(msg maelstrom.Message) error {
	n.broadcastMtx.Lock()
	defer n.broadcastMtx.Unlock()
	body := boardcastBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if !n.BroadcastHander.isIdKnown(body.BoardcastID) {
		n.BroadcastHander.addBroadcastId(body.BoardcastID, &body)
		n.addNumToReceivedBC(body.Message)
	}
	respBody := map[string]string{
		"type":         "broadcast_ok",
		"braodcast_id": body.BoardcastID,
	}
	return n.node.Reply(msg, respBody)
}

func (n *NodeWrapper) broadcastOk(msg maelstrom.Message) error {
	var body struct {
		Type        string `json:"type"`
		BroadcastID string `json:"braodcast_id"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	n.BroadcastHander.markBroadcastAsDoneForDst(msg.Src, body.BroadcastID)
	return nil
}
