package storage

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Mode string

const (
	SIMPLE    Mode = "SIMPLE"
	IMMUTABLE Mode = "IMMUTABLE"
)

func (m Mode) IsValid() bool {
	switch m {
	case SIMPLE, IMMUTABLE:
		return true
	}

	return false
}

type Configuration struct {
	Mode Mode `yaml:"mode"`
	Api  struct {
		Port int `yaml:"port"`
	} `yaml:"api"`
	DataBase struct {
		Host              string `yaml:"host"`
		Port              int    `yaml:"port"`
		User              string
		Password          string
		MaintenanceDBName string `yaml:"maintenance_db_name"`
		AppDBName         string `yaml:"app_db_name"`
	} `yaml:"database"`
	InputDir string `yaml:"input_dir"`
}

func ReadConfiguration(filepath string) *Configuration {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Failed to open configuration file: %v", err)
	}
	defer f.Close()

	var config Configuration
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatalf("Failed to read configuration: %v", err)
	}

	if !config.Mode.IsValid() {
		log.Fatalf("Unsupported mode specified: %s", config.Mode)
	}

	config.DataBase.User = getenv("DB_USER", "postgres")
	config.DataBase.Password = getenv("DB_PASSWORD", "admin")

	return &config
}

func getenv(key string, fallback string) string {
	val := os.Getenv(key)
	if len(val) > 0 {
		return val
	}

	return fallback
}
