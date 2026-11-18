package storage

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeId, MachineId, DiskId, Listen, DataDir string
	Peers                                      map[string]string
	ScrubInterval                              time.Duration

	// Integração com o mestre (opcional): MASTER_ADDR liga o registro
	// automático e os heartbeats. ADVERTISE_ADDR é o endereço que clientes
	// e o mestre usam para falar com este nó (padrão: LISTEN_ADDR).
	MasterAddr, AdvertiseAddr string
	HeartbeatInterval         time.Duration
	// CAPACITY (ex.: 10GiB, 500MiB) simula um disco desse tamanho.
	Capacity uint64
}

// ParseBytes aceita "1048576", "512MiB", "10GiB", "10GB", "2TiB".
func ParseBytes(v string) (uint64, error) {
	v = strings.TrimSpace(v)
	units := []struct {
		suffix string
		mult   uint64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10},
		{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1}}
	mult := uint64(1)
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			v, mult = strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}

func ValidAddress(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " /\\\t\n") {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535
}

func LoadConfig() (Config, error) {
	c := Config{NodeId: os.Getenv("NODE_ID"), MachineId: os.Getenv("MACHINE_ID"),
		DiskId: os.Getenv("DISK_ID"), Listen: os.Getenv("LISTEN_ADDR"), DataDir: os.Getenv("DATA_DIR"),
		Peers: make(map[string]string), ScrubInterval: time.Minute}
	for name, id := range map[string]string{"NODE_ID": c.NodeId, "MACHINE_ID": c.MachineId, "DISK_ID": c.DiskId} {
		if !blockIDPattern.MatchString(id) {
			return c, fmt.Errorf("%s: ID inválido ou vazio", name)
		}
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return c, fmt.Errorf("DATA_DIR vazio")
	}
	if !ValidAddress(c.Listen) {
		return c, fmt.Errorf("LISTEN_ADDR inválido: use host:porta")
	}
	if value := os.Getenv("SCRUB_INTERVAL"); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil || d < 0 {
			return c, fmt.Errorf("SCRUB_INTERVAL inválido")
		}
		c.ScrubInterval = d
	}
	if value := os.Getenv("CAPACITY"); value != "" {
		n, err := ParseBytes(value)
		if err != nil || n == 0 {
			return c, fmt.Errorf("CAPACITY inválido: use bytes ou sufixo (ex.: 10GiB)")
		}
		c.Capacity = n
	}
	c.MasterAddr = os.Getenv("MASTER_ADDR")
	c.AdvertiseAddr = os.Getenv("ADVERTISE_ADDR")
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Listen
	}
	c.HeartbeatInterval = 3 * time.Second
	if value := os.Getenv("HEARTBEAT_INTERVAL"); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return c, fmt.Errorf("HEARTBEAT_INTERVAL inválido")
		}
		c.HeartbeatInterval = d
	}
	if c.MasterAddr != "" {
		if !ValidAddress(c.MasterAddr) {
			return c, fmt.Errorf("MASTER_ADDR inválido: use host:porta")
		}
		host, _, _ := net.SplitHostPort(c.AdvertiseAddr)
		if !ValidAddress(c.AdvertiseAddr) || host == "0.0.0.0" || host == "::" {
			return c, fmt.Errorf("ADVERTISE_ADDR inválido: informe o host:porta alcançável pelos clientes (LISTEN_ADDR escuta em %s)", c.Listen)
		}
	}
	if value := os.Getenv("PEERS"); value != "" {
		for _, part := range strings.Split(value, ",") {
			id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
			if !ok || !blockIDPattern.MatchString(id) || !ValidAddress(addr) {
				return c, fmt.Errorf("PEERS: use id=host:porta separados por vírgula")
			}
			if _, exists := c.Peers[id]; exists {
				return c, fmt.Errorf("PEERS: ID duplicado %s", id)
			}
			c.Peers[id] = addr
		}
	}
	return c, nil
}
