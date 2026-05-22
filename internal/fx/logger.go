package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
)

// LoggerModule provides the structured logger.
var LoggerModule = fx.Provide(ProvideLogger)

// ProvideLogger constructs the process logger.
func ProvideLogger(cfg *config.Config) (logging.Logger, error) {
	return logging.New(cfg.App.Name, cfg.App.Env, cfg.App.LogLevel)
}
