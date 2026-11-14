package storage

import (
	"errors"
	"github.com/caarlos0/env/v11"
)

type Config struct {
	NodeId     string `env:"NODE_ID"`
	MachineId  string `env:"MACHINE_ID"`
	DiskId     string `env:"DISK_ID"`
	Listen	   string `env:"LISTEN_ADDR"`
	DataDir    string `env:"DATA_DIR"`
}

func LoadConfig() (Config, error){

	cfg, err := env.ParseAs[Config]()


	if err != nil {
		return Config{}, errors.New("Erro na confuguracao do ambiente")
	}

	if cfg.NodeId=="" {
		return Config{}, errors.New("NodeID vazio")
	}
	if cfg.MachineId == "" {
		return Config{}, errors.New("MachineID vazio")
	}
	if cfg.DiskId == "" {
		return Config{}, errors.New("DiskId vazio")
	}
	if cfg.Listen == "" {
		return Config{}, errors.New("Listen vazio")
	}
	if cfg.DataDir == "" {
		return Config{}, errors.New("NodeID vazio")
	}

	return cfg, nil

}
