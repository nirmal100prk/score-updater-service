package config

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const (
	DEFAULT_CONFIG_FILE = "../internal/config/config.yaml"
)

type ServiceConfig struct {
	Name        string
	Host        string
	Port        int
	PostgresCfg DbConfig
	KafkaCfg    KafkaConfig
}

type DbConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	DbName   string
}

type KafkaConfig struct {
	Broker string
	Topic string
	GroupId string
}

func NewConfig() (*ServiceConfig, error) {

	cfg, err := loadConfig(DEFAULT_CONFIG_FILE)
	if err != nil {
		slog.Error("load service config error", "", err)
	}
	return cfg, err
}

func loadConfig(path string) (*ServiceConfig, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path = filepath.Dir(path)

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(path)
	//viper.AddConfigPath("/internal/config")

	viper.SetEnvKeyReplacer(strings.NewReplacer(`,`, `_`))
	// env settings
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`))
	viper.AutomaticEnv()
	config := &ServiceConfig{}

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("viper failed to parse config: %w", err)
	}
	return config, nil
}
