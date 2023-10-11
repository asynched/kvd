package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	RaftPort  int    `json:"raft_port"`
	Bootstrap bool   `json:"bootstrap"`
	JoinAddr  string `json:"join_addr"`
}

func ParseConfig(filename string) (Config, error) {
	var config Config

	file, err := os.Open(filename)

	if err != nil {
		return config, err
	}

	defer file.Close()

	err = json.NewDecoder(file).Decode(&config)

	if err != nil {
		return config, err
	}

	if config.Name == "" {
		return config, fmt.Errorf("name is required")
	}

	if config.Host == "" {
		return config, fmt.Errorf("host is required")
	}

	if config.Port == 0 {
		return config, fmt.Errorf("port is required")
	}

	if config.RaftPort == 0 {
		return config, fmt.Errorf("raft_port is required")
	}

	if config.Bootstrap && config.JoinAddr != "" {
		return config, fmt.Errorf("bootstrap and join_addr cannot be set at the same time")
	}

	return config, nil
}
