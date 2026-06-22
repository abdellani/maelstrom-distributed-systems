package main

import (
	"log"
)

func main() {
	n := NewNode()
	if err := n.node.Run(); err != nil {
		log.Fatal(err)
	}
}
