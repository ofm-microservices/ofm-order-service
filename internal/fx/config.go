package appfx

import (
	"go.uber.org/fx"
	"order-service/config"
)

// ConfigModule loads the order-service configuration.
var ConfigModule = fx.Provide(config.Load)
