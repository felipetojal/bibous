package master

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("conflict")
	ErrInvalid     = errors.New("invalid request")
	ErrUnavailable = errors.New("insufficient healthy storage nodes")
)

// Config define os parâmetros configuráveis do controlador.
type Config struct {
	DefaultReplicas   int
	SuspectAfter      time.Duration
	DeadAfter         time.Duration
	ScanInterval      time.Duration
	UploadTTL         time.Duration
	MaxDiskUsage      float64
	MaxRepairsPerScan int
	RelayRecovery     bool
}

// DefaultConfig estabelece valores adequados para um cluster de laboratório.
func DefaultConfig() Config {
	return Config{
		DefaultReplicas: 3, SuspectAfter: 9 * time.Second,
		DeadAfter: 15 * time.Second, ScanInterval: 3 * time.Second,
		UploadTTL: 10 * time.Minute, MaxDiskUsage: 0.90,
		MaxRepairsPerScan: 2, RelayRecovery: true,
	}
}

// BlockTransport isola o acesso aos blocos do restante do mestre.
type BlockTransport interface {
	ReadBlock(ctx context.Context, n NodeInfo, block BlockMetadata) ([]byte, error)
	WriteBlock(ctx context.Context, n NodeInfo, block BlockMetadata, contents []byte) error
}

// Master coordena o espaço de nomes persistente e o registro volátil dos nós.
type Master struct {
	mu        sync.Mutex
	repairMu  sync.Mutex
	nr        *NodeRegistry
	db        *MetadataDB
	state     MetadataState
	cfg       Config
	transport BlockTransport
}

// NewMaster restaura os metadados persistidos e inicializa o controlador.
func NewMaster(dir string, cfg Config, transport BlockTransport) (*Master, error) {
	if cfg.DefaultReplicas < 1 || cfg.SuspectAfter <= 0 || cfg.DeadAfter <= cfg.SuspectAfter ||
		cfg.ScanInterval <= 0 || cfg.UploadTTL <= 0 || cfg.MaxDiskUsage <= 0 || cfg.MaxDiskUsage > 1 || cfg.MaxRepairsPerScan < 1 {
		return nil, errors.New("invalid master configuration")
	}
	db, state, err := NewMetadataDB(dir)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, errors.New("block transport required")
	}
	return &Master{nr: NewNodeRegistry(), db: db, state: state, cfg: cfg, transport: transport}, nil
}

// RegisterNode insere ou atualiza a identificação de um nó no registro volátil.
func (m *Master) RegisterNode(info NodeInfo) error { return m.nr.Register(info) }

// Heartbeat atualiza a presença do nó e opcionalmente sua capacidade.
func (m *Master) Heartbeat(id string, free *uint64) error { return m.nr.Heartbeat(id, free) }

// Nodes devolve uma cópia ordenada dos registros conhecidos.
func (m *Master) Nodes() []NodeInfo { return m.nr.Snapshot() }

// DrainNode impede novas alocações para um nó em retirada.
func (m *Master) DrainNode(id string) error { return m.nr.Drain(id) }

// saveLocked publica um novo estado apenas depois da gravação persistente.
// Quem chama deve possuir m.mu e não modificar m.state diretamente.
func (m *Master) saveLocked(next MetadataState) error {
	if err := m.db.Save(next); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}
	m.state = next
	return nil
}

func newID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func checkSHA(value string) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == sha256.Size && value == strings.ToLower(value)
}

// capacityReservations contabiliza as reservas dos uploads não confirmados.
func (m *Master) capacityReservations() map[string]uint64 {
	reserved := map[string]uint64{}
	for _, upload := range m.state.Pending {
		if !time.Now().Before(upload.ExpiresAt) {
			continue
		}
		for _, b := range upload.File.Blocks {
			for _, id := range b.Replicas {
				reserved[id] += b.Length
			}
		}
	}
	return reserved
}

func eligible(n NodeInfo, reserved, blockSize uint64, maxUsage float64) bool {
	if n.Status != NodeAlive || n.TotalSize == 0 {
		return false
	}
	if n.AvailableSize < reserved || n.AvailableSize-reserved < blockSize {
		return false
	}
	// Considera a utilização prevista após a nova gravação.
	projectedAvailable := n.AvailableSize - reserved - blockSize
	return float64(n.TotalSize-projectedAvailable)/float64(n.TotalSize) <= maxUsage
}

func (m *Master) chooseNodes(size uint64, count int, reserved map[string]uint64, excluded map[string]bool) ([]string, error) {
	nodes := m.nr.Snapshot()
	sort.SliceStable(nodes, func(i, j int) bool {
		freeI, freeJ := nodes[i].AvailableSize, nodes[j].AvailableSize
		if reserved[nodes[i].ID] <= freeI {
			freeI -= reserved[nodes[i].ID]
		} else {
			freeI = 0
		}
		if reserved[nodes[j].ID] <= freeJ {
			freeJ -= reserved[nodes[j].ID]
		} else {
			freeJ = 0
		}
		if freeI == freeJ {
			return nodes[i].ID < nodes[j].ID
		}
		return freeI > freeJ
	})
	usedMachines := map[string]bool{}
	usedDisks := map[string]bool{}
	for _, n := range nodes {
		if excluded[n.ID] {
			usedMachines[n.MachineID] = true
			usedDisks[n.MachineID+"/"+n.DiskID] = true
		}
	}
	selected := make([]string, 0, count)
	for _, n := range nodes {
		if excluded[n.ID] || usedMachines[n.MachineID] || usedDisks[n.MachineID+"/"+n.DiskID] {
			continue
		}
		if !eligible(n, reserved[n.ID], size, m.cfg.MaxDiskUsage) {
			continue
		}
		selected = append(selected, n.ID)
		usedMachines[n.MachineID] = true
		usedDisks[n.MachineID+"/"+n.DiskID] = true
		if len(selected) == count {
			break
		}
	}
	if len(selected) != count {
		return nil, ErrUnavailable
	}
	for _, id := range selected {
		reserved[id] += size
	}
	return selected, nil
}

// PrepareUpload valida o manifesto e reserva posições físicas para cada bloco.
func (m *Master) PrepareUpload(p string, size uint64, fileHash string, specs []BlockSpec, replicas int) (PendingUpload, error) {
	if err := validatePath(p, false); err != nil {
		return PendingUpload{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !checkSHA(fileHash) || len(specs) == 0 {
		return PendingUpload{}, fmt.Errorf("%w: invalid file hash or empty block manifest", ErrInvalid)
	}
	if replicas == 0 {
		replicas = m.cfg.DefaultReplicas
	}
	if replicas < 1 || replicas > 100 {
		return PendingUpload{}, fmt.Errorf("%w: replication factor", ErrInvalid)
	}
	var sum uint64
	for i, b := range specs {
		if b.Size > MaxBlockSize || !checkSHA(b.Hash) || (b.Size == 0 && (size != 0 || len(specs) != 1)) || (b.Size > 0 && size == 0) {
			return PendingUpload{}, fmt.Errorf("%w: invalid block %d", ErrInvalid, i)
		}
		if sum > size || b.Size > size-sum {
			return PendingUpload{}, fmt.Errorf("%w: inconsistent file size", ErrInvalid)
		}
		sum += b.Size
	}
	if sum != size {
		return PendingUpload{}, fmt.Errorf("%w: block lengths do not match file size", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.state.Directories[path.Dir(p)] {
		return PendingUpload{}, ErrNotFound
	}
	if _, exists := m.state.Files[p]; exists {
		return PendingUpload{}, ErrConflict
	}
	for _, u := range m.state.Pending {
		if u.File.Path == p && time.Now().Before(u.ExpiresAt) {
			return PendingUpload{}, ErrConflict
		}
	}
	reserved := m.capacityReservations()
	blocks := make([]BlockMetadata, 0, len(specs))
	for _, spec := range specs {
		ids, err := m.chooseNodes(spec.Size, replicas, reserved, nil)
		if err != nil {
			return PendingUpload{}, err
		}
		id, err := newID()
		if err != nil {
			return PendingUpload{}, err
		}
		blocks = append(blocks, BlockMetadata{ID: id, Version: 1, Length: spec.Size, Hash: spec.Hash, Replicas: ids})
	}
	id, err := newID()
	if err != nil {
		return PendingUpload{}, err
	}
	now := time.Now()
	upload := PendingUpload{ID: id, ExpiresAt: now.Add(m.cfg.UploadTTL), File: FileMetadata{
		Path: p, Size: size, Hash: fileHash, ReplicationFactor: replicas, CreatedAt: now, UpdatedAt: now, Blocks: blocks,
	}}
	next := cloneState(m.state)
	next.Pending[id] = upload
	if err := m.saveLocked(next); err != nil {
		return PendingUpload{}, err
	}
	return upload, nil
}

// CommitUpload verifica todas as réplicas alocadas usando GetBlock e
// reconstrói o SHA-256 do arquivo completo. A verificação evita confiar
// apenas na confirmação do cliente, porém trafega os blocos pelo mestre
// enquanto o armazenamento não disponibilizar VerifyBlock.
func (m *Master) CommitUpload(ctx context.Context, id string) (FileMetadata, error) {
	m.mu.Lock()
	upload, ok := m.state.Pending[id]
	m.mu.Unlock()
	if !ok {
		return FileMetadata{}, ErrNotFound
	}
	if !time.Now().Before(upload.ExpiresAt) {
		return FileMetadata{}, ErrConflict
	}
	var fileHash hash.Hash = sha256.New()
	for _, block := range upload.File.Blocks {
		for j, nodeID := range block.Replicas {
			n, exists := m.nr.Get(nodeID)
			if !exists || n.Status != NodeAlive {
				return FileMetadata{}, ErrUnavailable
			}
			content, err := m.transport.ReadBlock(ctx, n, block)
			if err != nil {
				return FileMetadata{}, fmt.Errorf("verify replica %s/%s: %w", nodeID, block.ID, err)
			}
			if uint64(len(content)) != block.Length {
				return FileMetadata{}, fmt.Errorf("replica %s has incorrect length", nodeID)
			}
			digest := sha256.Sum256(content)
			if hex.EncodeToString(digest[:]) != block.Hash {
				return FileMetadata{}, fmt.Errorf("replica %s has incorrect hash", nodeID)
			}
			if j == 0 {
				_, _ = fileHash.Write(content)
			}
		}
	}
	if hex.EncodeToString(fileHash.Sum(nil)) != upload.File.Hash {
		return FileMetadata{}, fmt.Errorf("%w: full-file SHA-256 differs from block contents", ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.state.Pending[id]
	if !ok || !time.Now().Before(current.ExpiresAt) {
		return FileMetadata{}, ErrConflict
	}
	if _, exists := m.state.Files[current.File.Path]; exists {
		return FileMetadata{}, ErrConflict
	}
	next := cloneState(m.state)
	delete(next.Pending, id)
	next.Files[current.File.Path] = current.File
	if err := m.saveLocked(next); err != nil {
		return FileMetadata{}, err
	}
	return current.File, nil
}

// AbortUpload libera uma reserva lógica que não foi confirmada.
func (m *Master) AbortUpload(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state.Pending[id]; !ok {
		return ErrNotFound
	}
	next := cloneState(m.state)
	delete(next.Pending, id)
	return m.saveLocked(next)
}

// GetFile recupera os metadados de um arquivo já confirmado.
func (m *Master) GetFile(p string) (FileMetadata, error) {
	if err := validatePath(p, false); err != nil {
		return FileMetadata{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.state.Files[p]
	if !ok {
		return FileMetadata{}, ErrNotFound
	}
	return cloneFile(f), nil
}

func cloneFile(f FileMetadata) FileMetadata {
	f.Blocks = append([]BlockMetadata(nil), f.Blocks...)
	for i := range f.Blocks {
		f.Blocks[i].Replicas = append([]string(nil), f.Blocks[i].Replicas...)
		f.Blocks[i].BadReplicas = append([]string(nil), f.Blocks[i].BadReplicas...)
	}
	return f
}

// FileStatus calcula a situação atual com base nas réplicas disponíveis.
func (m *Master) FileStatus(f FileMetadata) string {
	if len(f.Blocks) == 0 {
		return "UNAVAILABLE"
	}
	minActive := f.ReplicationFactor
	allReadable := true
	for _, b := range f.Blocks {
		active, readable := 0, 0
		for _, id := range b.Replicas {
			if n, ok := m.nr.Get(id); ok {
				if n.Status == NodeAlive {
					active++
					readable++
				}
				if n.Status == NodeDraining {
					readable++
				}
			}
		}
		if active < minActive {
			minActive = active
		}
		if readable == 0 {
			allReadable = false
		}
	}
	if !allReadable {
		return "UNAVAILABLE"
	}
	if minActive < f.ReplicationFactor {
		return "UNDER_REPLICATED"
	}
	return "HEALTHY"
}

// CreateDirectory acrescenta um diretório ao espaço de nomes persistente.
func (m *Master) CreateDirectory(p string) error {
	if err := validatePath(p, true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Directories[p] {
		return ErrConflict
	}
	if _, ok := m.state.Files[p]; ok {
		return ErrConflict
	}
	if !m.state.Directories[path.Dir(p)] {
		return ErrNotFound
	}
	next := cloneState(m.state)
	next.Directories[p] = true
	return m.saveLocked(next)
}

// DeleteDirectory remove apenas diretórios vazios, exceto a raiz.
func (m *Master) DeleteDirectory(p string) error {
	if err := validatePath(p, true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if p == "/" {
		return ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.state.Directories[p] {
		return ErrNotFound
	}
	for dir := range m.state.Directories {
		if path.Dir(dir) == p {
			return ErrConflict
		}
	}
	for file := range m.state.Files {
		if path.Dir(file) == p {
			return ErrConflict
		}
	}
	for _, upload := range m.state.Pending {
		if path.Dir(upload.File.Path) == p {
			return ErrConflict
		}
	}
	next := cloneState(m.state)
	delete(next.Directories, p)
	return m.saveLocked(next)
}

// ListDirectory devolve os arquivos e subdiretórios imediatos.
func (m *Master) ListDirectory(p string) ([]string, []string, error) {
	if err := validatePath(p, true); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.state.Directories[p] {
		return nil, nil, ErrNotFound
	}
	var directories, files []string
	for dir := range m.state.Directories {
		if dir != p && path.Dir(dir) == p {
			directories = append(directories, dir)
		}
	}
	for file := range m.state.Files {
		if path.Dir(file) == p {
			files = append(files, file)
		}
	}
	sort.Strings(directories)
	sort.Strings(files)
	return directories, files, nil
}

// MoveFile altera o caminho lógico do arquivo sem transferir blocos.
func (m *Master) MoveFile(src, dst string) error {
	if err := validatePath(src, false); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := validatePath(dst, false); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.state.Files[src]
	if !ok {
		return ErrNotFound
	}
	if _, exists := m.state.Files[dst]; exists || !m.state.Directories[path.Dir(dst)] {
		return ErrConflict
	}
	for _, up := range m.state.Pending {
		if up.File.Path == src || up.File.Path == dst {
			return ErrConflict
		}
	}
	next := cloneState(m.state)
	delete(next.Files, src)
	f.Path = dst
	f.UpdatedAt = time.Now()
	next.Files[dst] = f
	return m.saveLocked(next)
}

// DeleteFile remove somente os metadados: o StorageService atual não possui
// DeleteBlock. Os blocos físicos órfãos exigirão limpeza futura.
func (m *Master) DeleteFile(p string) error {
	if err := validatePath(p, false); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state.Files[p]; !ok {
		return ErrNotFound
	}
	next := cloneState(m.state)
	delete(next.Files, p)
	return m.saveLocked(next)
}

// ReportCorruption invalida a referência de uma réplica denunciada.
func (m *Master) ReportCorruption(nodeID, blockID string, version int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := cloneState(m.state)
	changed := false
	for p, f := range next.Files {
		for i, b := range f.Blocks {
			if b.ID != blockID || b.Version != version {
				continue
			}
			for j, id := range b.Replicas {
				if id == nodeID {
					f.Blocks[i].Replicas = append(b.Replicas[:j:j], b.Replicas[j+1:]...)
					f.Blocks[i].BadReplicas = append(f.Blocks[i].BadReplicas, nodeID)
					next.Files[p] = f
					changed = true
					break
				}
			}
		}
	}
	if !changed {
		return ErrNotFound
	}
	return m.saveLocked(next)
}

// ClusterStatus apresenta os nós e o estado de replicação do cluster.
func (m *Master) ClusterStatus() (nodes []NodeInfo, healthy, under, unavailable, pending int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes = m.nr.Snapshot()
	for _, f := range m.state.Files {
		switch m.FileStatus(f) {
		case "HEALTHY":
			healthy++
		case "UNDER_REPLICATED":
			under++
		default:
			unavailable++
		}
	}
	return nodes, healthy, under, unavailable, len(m.state.Pending)
}

// MaintenanceTick executa tarefas de rotina e também pode ser chamado em testes.
func (m *Master) MaintenanceTick(ctx context.Context, now time.Time) {
	for _, id := range m.nr.Reap(now, m.cfg.SuspectAfter, m.cfg.DeadAfter) {
		log.Printf("node dead node_id=%s", id)
	}
	m.mu.Lock()
	next := cloneState(m.state)
	removed := false
	for id, upload := range next.Pending {
		if !now.Before(upload.ExpiresAt) {
			delete(next.Pending, id)
			removed = true
		}
	}
	if removed {
		if err := m.saveLocked(next); err != nil {
			log.Printf("expire uploads: %v", err)
		}
	}
	m.mu.Unlock()
	if m.cfg.RelayRecovery {
		if err := m.RepairOnce(ctx); err != nil {
			log.Printf("repair: %v", err)
		}
	}
}

// RunMaintenance mantém a detecção de falhas, a expiração e a recuperação.
func (m *Master) RunMaintenance(ctx context.Context) {
	timer := time.NewTicker(m.cfg.ScanInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			m.MaintenanceTick(ctx, now)
		}
	}
}

// ReplicaTask representa uma cópia ausente. A fila prioriza blocos com
// menos réplicas disponíveis; a recuperação usa GetBlock e PutBlock
type ReplicaTask struct {
	FilePath    string
	Block       BlockMetadata
	Source      NodeInfo
	Destination NodeInfo
	LiveCount   int
}

func (m *Master) planRepairsLocked() []ReplicaTask {
	reserved := m.capacityReservations()
	tasks := make([]ReplicaTask, 0)
	for p, f := range m.state.Files {
		for _, b := range f.Blocks {
			active := 0
			var source NodeInfo
			for _, id := range b.Replicas {
				if n, ok := m.nr.Get(id); ok {
					if n.Status == NodeAlive {
						active++
						source = n
					}
					if n.Status == NodeDraining && source.ID == "" {
						source = n
					}
				}
			}
			if source.ID == "" || active >= f.ReplicationFactor {
				continue
			}
			excluded := map[string]bool{}
			for _, id := range b.Replicas {
				excluded[id] = true
			}
			for _, id := range b.BadReplicas {
				excluded[id] = true
			}
			ids, err := m.chooseNodes(b.Length, 1, reserved, excluded)
			if err != nil {
				continue
			}
			dst, _ := m.nr.Get(ids[0])
			tasks = append(tasks, ReplicaTask{p, b, source, dst, active})
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].LiveCount != tasks[j].LiveCount {
			return tasks[i].LiveCount < tasks[j].LiveCount
		}
		if tasks[i].FilePath != tasks[j].FilePath {
			return tasks[i].FilePath < tasks[j].FilePath
		}
		return tasks[i].Block.ID < tasks[j].Block.ID
	})
	if len(tasks) > m.cfg.MaxRepairsPerScan {
		tasks = tasks[:m.cfg.MaxRepairsPerScan]
	}
	return tasks
}

// PlannedRepairs permite inspecionar as cópias que faltam.
func (m *Master) PlannedRepairs() []ReplicaTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.planRepairsLocked()
}

// RepairOnce executa uma rodada limitada de recuperação de blocos.
func (m *Master) RepairOnce(ctx context.Context) error {
	// Somente uma execução de recuperação pode operar por vez. A comunicação
	// de rede ocorre fora do bloqueio de metadados para evitar gargalos.
	m.repairMu.Lock()
	defer m.repairMu.Unlock()
	m.mu.Lock()
	tasks := m.planRepairsLocked()
	m.mu.Unlock()
	var failures []error
	for _, t := range tasks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		data, err := m.transport.ReadBlock(ctx, t.Source, t.Block)
		if err == nil {
			digest := sha256.Sum256(data)
			if uint64(len(data)) != t.Block.Length || hex.EncodeToString(digest[:]) != t.Block.Hash {
				err = errors.New("source block corrupt")
			}
		}
		if err == nil {
			err = m.transport.WriteBlock(ctx, t.Destination, t.Block, data)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("repair block=%s destination=%s: %w", t.Block.ID, t.Destination.ID, err))
			continue
		}
		m.mu.Lock()
		f, exists := m.state.Files[t.FilePath]
		if !exists {
			m.mu.Unlock()
			continue
		} // O arquivo foi removido ou movido durante a cópia; pode sobrar um bloco órfão.
		index := -1
		for i, b := range f.Blocks {
			if b.ID == t.Block.ID && b.Version == t.Block.Version {
				index = i
				break
			}
		}
		if index < 0 {
			m.mu.Unlock()
			continue
		}
		dest, alive := m.nr.Get(t.Destination.ID)
		if !alive || dest.Status != NodeAlive {
			m.mu.Unlock()
			continue
		}
		b := f.Blocks[index]
		already := false
		for _, id := range b.Replicas {
			if id == dest.ID {
				already = true
			}
		}
		if already {
			m.mu.Unlock()
			continue
		}
		// Mantém réplicas saudáveis e descarta referências de nós mortos ou em drenagem.
		ids := make([]string, 0, len(b.Replicas)+1)
		for _, id := range b.Replicas {
			if n, ok := m.nr.Get(id); ok && n.Status == NodeAlive {
				ids = append(ids, id)
			}
		}
		ids = append(ids, dest.ID)
		next := cloneState(m.state)
		f = next.Files[t.FilePath]
		f.Blocks[index].Replicas = ids
		next.Files[t.FilePath] = f
		err = m.saveLocked(next)
		m.mu.Unlock()
		if err != nil {
			failures = append(failures, err)
			continue
		}
		log.Printf("replica repaired file=%s block=%s destination=%s", t.FilePath, t.Block.ID, dest.ID)
	}
	return errors.Join(failures...)
}
