package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config groups order-service runtime settings.
type Config struct {
	App     AppConfig
	GRPC    GRPCConfig
	File    FileServiceConfig
	User    UserServiceConfig
	DB      DBConfig
	Redis   RedisConfig
	NATS    NATSConfig
	Metrics MetricsConfig
	Tracing TracingConfig
}

// Load reads environment variables into Config.
func Load() (*Config, error) {
	_ = godotenv.Load()
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, WrapParseEnvConfigError(err)
	}
	return cfg, nil
}
