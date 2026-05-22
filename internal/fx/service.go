package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	"order-service/internal/application"
	"order-service/internal/domain"
)

// ServiceModule provides the order application service.
var ServiceModule = fx.Options(fx.Provide(ProvideOrderService))

// ProvideOrderService constructs the order application service.
func ProvideOrderService(writeRepo domain.OrderRepository, readRepo domain.OrderReadRepository, broker application.EventBroker, cfg *config.Config, lg logging.Logger) (application.Service, error) {
	return application.New(writeRepo, readRepo, broker, application.Config{
		OrderCreateResultSubject: cfg.NATS.OrderCreateResultSubject,
	}, lg)
}
