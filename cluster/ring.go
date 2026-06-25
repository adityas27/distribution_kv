package cluster

import (
	"fmt"
	"sort"
)

type HashRing struct {
	virtualNodes int
	ring         map[uint32]Node
	sortedHashes []uint32
	nodes         map[string]Node
}

func NewHashRing(virtualNodes int) *HashRing {
	return &HashRing{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]Node),
		sortedHashes: make([]uint32, 0),
		nodes:        make(map[string]Node),
	}
}

func (h *HashRing) AddNode(node Node) {
	h.nodes[node.ID] = node

	for i := 0; i < h.virtualNodes; i++ {
		key := fmt.Sprintf("%s#%d", node.ID, i)
		hash := Hash(key)

		h.ring[hash] = node
		h.sortedHashes = append(h.sortedHashes, hash)
	}

	sort.Slice(h.sortedHashes, func(i, j int) bool {
		return h.sortedHashes[i] < h.sortedHashes[j]
	})
}

func (h *HashRing) RemoveNode(id string) {
	delete(h.nodes, id)

	newHashes := make([]uint32, 0)

	for hash, node := range h.ring {
		if node.ID == id {
			delete(h.ring, hash)
			continue
		}
		newHashes = append(newHashes, hash)
	}

	sort.Slice(newHashes, func(i, j int) bool {
		return newHashes[i] < newHashes[j]
	})

	h.sortedHashes = newHashes
}

func (h *HashRing) GetNode(key string) Node {
	if len(h.sortedHashes) == 0 {
		return Node{}
	}

	hash := Hash(key)

	idx := sort.Search(len(h.sortedHashes), func(i int) bool {
		return h.sortedHashes[i] >= hash
	})

	if idx == len(h.sortedHashes) {
		idx = 0
	}

	return h.ring[h.sortedHashes[idx]]
}

func (h *HashRing) GetNodes() []Node {
	nodes := make([]Node, 0, len(h.nodes))

	for _, node := range h.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}