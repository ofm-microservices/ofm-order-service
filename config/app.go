package config

// AppConfig defines runtime metadata.
type AppConfig struct {
	Name     string `env:"APP_NAME" envDefault:"order-service"`
	Env      string `env:"APP_ENV" envDefault:"local"`
	LogLevel string `env:"APP_LOG_LEVEL" envDefault:"info"`
}
