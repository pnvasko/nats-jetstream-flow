package common

import (
	"fmt"
	"os"
	"strings"
)

type OtlpConfig interface {
	Debug() bool
	Environment() string
	Dsn() string
	ServiceName() string
	Version() string
	Key() string
}
type DevOtlpConfig struct {
	debug       bool   `yaml:"debug"`
	dsn         string `yaml:"dsn"`
	serviceName string `yaml:"serviceName"`
	environment string `yaml:"environment"`
	version     string `yaml:"version"`
	key         string `yaml:"key"`
}

func NewDevOtlpConfig() (*DevOtlpConfig, error) {
	cfg := &DevOtlpConfig{}
	cfg.debug = false
	debug := strings.ToLower(os.Getenv("Debug"))
	if debug == "true" {
		cfg.debug = true
	}

	cfg.dsn = os.Getenv("DSN")
	if cfg.dsn == "" {
		return nil, fmt.Errorf("DSN is required")
	}

	cfg.serviceName = os.Getenv("SERVICE_NAME")
	if cfg.serviceName == "" {
		return nil, fmt.Errorf("SERVICE_NAME is required")

	}

	cfg.environment = os.Getenv("ENVIRONMENT")
	if cfg.environment == "" {
		return nil, fmt.Errorf("ENVIRONMENT is required")

	}

	cfg.version = os.Getenv("VERSION")
	if cfg.version == "" {
		return nil, fmt.Errorf("VERSION is required")
	}

	cfg.key = os.Getenv("KEY")
	if cfg.key == "" {
		return nil, fmt.Errorf("KEY is required")
	}
	return cfg, nil
}

func (d *DevOtlpConfig) Debug() bool {
	return d.debug
}

func (d *DevOtlpConfig) Environment() string {
	return d.environment
}

func (d *DevOtlpConfig) Dsn() string {
	return d.dsn
}

func (d *DevOtlpConfig) ServiceName() string {
	return d.serviceName
}

func (d *DevOtlpConfig) Version() string {
	return d.version
}

func (d *DevOtlpConfig) Key() string {
	return d.key
}

var _ OtlpConfig = (*DevOtlpConfig)(nil)
