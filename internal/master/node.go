package master

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type NodeStatus string

const (
	NodeAlive    NodeStatus = "ALIVE"
	NodeSuspect  NodeStatus = "SUSPECT"
	NodeDead     NodeStatus = "DEAD"
	NodeDraining NodeStatus = "DRAINING"
)

// NodeInfo registra a identidade do processo e seus domínios de falha.
type NodeInfo struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"`
	MachineID     string     `json:"machine_id"`
	DiskID        string     `json:"disk_id"`
	TotalSize     uint64     `json:"total_size"`
	AvailableSize uint64     `json:"available_size"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	Status        NodeStatus `json:"status"`
}

type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]NodeInfo
}

// NewNodeRegistry inicializa o mapa concorrente de nós.
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: make(map[string]NodeInfo)}
}

// Register valida a identidade física e registra o último contato.
func (r *NodeRegistry) Register(n NodeInfo) error {
	if n.ID == "" || n.Address == "" || n.MachineID == "" || n.DiskID == "" {
		return errors.New("id, address, machine_id and disk_id are required")
	}
	if n.TotalSize == 0 || n.AvailableSize > n.TotalSize {
		return errors.New("invalid disk capacity")
	}
	n.LastHeartbeat = time.Now()
	n.Status = NodeAlive
	r.mu.Lock()
	defer r.mu.Unlock()
	// Nunca reutiliza um identificador para outra máquina ou disco,
	// mesmo após timeout: o nó antigo pode continuar referenciado nos metadados.
	if old, found := r.nodes[n.ID]; found {
		if old.MachineID != n.MachineID || old.DiskID != n.DiskID {
			return errors.New("node ID is already registered for another disk or machine")
		}
	}
	r.nodes[n.ID] = n
	return nil
}

// Heartbeat recebe o sinal de vida e atualiza o espaço informado.
func (r *NodeRegistry) Heartbeat(id string, available *uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return errors.New("unknown node; register first")
	}
	if available != nil {
		if *available > n.TotalSize {
			return errors.New("available space exceeds total space")
		}
		n.AvailableSize = *available
	}
	n.LastHeartbeat = time.Now()
	if n.Status != NodeDraining {
		n.Status = NodeAlive
	}
	r.nodes[id] = n
	return nil
}

// Reap classifica nós suspeitos ou mortos a partir dos timeouts.
func (r *NodeRegistry) Reap(now time.Time, suspectAfter, deadAfter time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var newlyDead []string
	for id, n := range r.nodes {
		age := now.Sub(n.LastHeartbeat)
		if age >= deadAfter {
			if n.Status != NodeDead {
				newlyDead = append(newlyDead, id)
			}
			n.Status = NodeDead
		} else if age >= suspectAfter && n.Status != NodeDraining {
			n.Status = NodeSuspect
		}
		r.nodes[id] = n
	}
	sort.Strings(newlyDead)
	return newlyDead
}

// Drain impede novas alocações e permite planejar réplicas substitutas.
func (r *NodeRegistry) Drain(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return errors.New("unknown node")
	}
	if n.Status == NodeDead {
		return errors.New("cannot drain a dead node")
	}
	n.Status = NodeDraining
	r.nodes[id] = n
	return nil
}

// Get consulta um nó por identificador sob bloqueio de leitura.
func (r *NodeRegistry) Get(id string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return n, ok
}

// Snapshot retorna uma cópia ordenada para posicionamento e relatórios.
func (r *NodeRegistry) Snapshot() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}
