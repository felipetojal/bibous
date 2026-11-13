package master

import (
	"sync"
	"time"
)

// NodeInfo encapsula os dados do nó.
type NodeInfo struct {
	id            string
	ipAddr        string
	totalSize     uint64
	availableSize uint64
	lastHeartbeat time.Time
}

// NodeRegistry mantém as informações
// sobre os nodes atuais.
type NodeRegistry struct {
	count uint64
	mu    sync.RWMutex
	nodes map[string]*NodeInfo
}

// NewNodeRegistry cria uma nova instância de NodeRegistry.
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{
		count: 0,
		mu: sync.RWMutex{},
		nodes: make(map[string]*NodeInfo),
	}
}

// NewNodeInfo cria uma nova instância de NodeInfo
func NewNodeInfo(id, ipAddr string, totalSize, availableSize uint64) *NodeInfo {
	return &NodeInfo{
		id: id,
		ipAddr: ipAddr,
		totalSize: totalSize,
		availableSize: availableSize,
		lastHeartbeat: time.Now(),
	}
}

// AddNode adiciona um NodeInfo ao NodeRegistry.
func (nr *NodeRegistry) AddNode(ni *NodeInfo) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.nodes[ni.id] = ni
}
