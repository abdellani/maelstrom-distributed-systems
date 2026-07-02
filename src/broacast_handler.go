package main

import (
	"sync"
	"time"
)

type BroadcastHandler struct {
	n        *NodeWrapper
	mtx      sync.RWMutex
	status   map[string]bool            // broadcast_id -> bool (true = broadcast done for id)
	progress map[string]map[string]bool // [node_id][bordcasti_d] -> bool (true = done)
}

func NewBroadcastHandler(n *NodeWrapper) *BroadcastHandler {
	return &BroadcastHandler{
		n:        n,
		status:   map[string]bool{},
		progress: map[string]map[string]bool{},
	}
}
func (b *BroadcastHandler) setNeighbors(ids []string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	for _, id := range ids {
		b._addNeighborsProgress(id)
	}
}
func (b *BroadcastHandler) _addNeighborsProgress(id string) {
	b.progress[id] = map[string]bool{}
}

func (b *BroadcastHandler) addBroadcastId(id string, body *boardcastBody) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	_, ok := b.status[id]
	if ok { //already in the loop
		return
	}
	b.status[id] = false
	for n := range b.progress {
		b.progress[n][id] = false
	}
	go b.broadcast(id, body)
}

func (b *BroadcastHandler) broadcast(id string, body *boardcastBody) {
	for {
		pendingNeighbors := []string{}
		b.mtx.RLock()
		if b.status[id] == true {
			b.mtx.RUnlock()
			return
		}

		for neighbor := range b.progress {
			status := b.progress[neighbor][id]
			if status == true {
				continue
			}
			pendingNeighbors = append(pendingNeighbors, neighbor)
		}
		b.mtx.RUnlock()
		if len(pendingNeighbors) == 0 { // done with this id
			b.mtx.Lock()
			b.status[id] = true
			b.mtx.Unlock()
			return
		}
		for _, neighbor := range pendingNeighbors {
			b.n.send(neighbor, *body)
		}
		time.Sleep(time.Microsecond * 200)
	}
}

func (b *BroadcastHandler) markBroadcastAsDoneForDst(neighbor string, bcstId string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.progress[neighbor][bcstId] = true
}

func (b *BroadcastHandler) isIdKnown(bcstId string) bool {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	_, ok := b.status[bcstId]
	return ok
}
