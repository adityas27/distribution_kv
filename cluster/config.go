package cluster

import (
	"encoding/json"
	"os"
)

type Config struct {
	VirtualNodes int    `json:"virtual_nodes"`
	Nodes        []Node `json:"nodes"`
}

func LoadConfig(path string) (*HashRing, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	ring := NewHashRing(cfg.VirtualNodes)

	for _, node := range cfg.Nodes {
		ring.AddNode(node)
	}

	return ring, nil
}