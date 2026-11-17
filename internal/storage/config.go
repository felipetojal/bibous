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
