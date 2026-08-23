package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	"order-service/internal/application"
	"order-service/internal/domain"
	filegrpc "order-service/internal/infra/file/grpc"
	usergrpc "order-service/internal/infra/user/grpc"
)

// ServiceModule provides the order application service.
var ServiceModule = fx.Options(fx.Provide(ProvideOrderService))

// ProvideOrderService constructs the order application service.
func ProvideOrderService(writeRepo domain.OrderRepository, readRepo domain.OrderReadRepository, files filegrpc.FileService, users usergrpc.UserService, broker application.EventBroker, cfg *config.Config, lg logging.Logger) (application.Service, error) {
	return application.New(writeRepo, readRepo, files, users, broker, application.Config{
		OrderCreateResultSubject:           cfg.Kafka.CreateResultTopic,
		OrderPreviewProjectionSubject:      cfg.Kafka.PreviewTopic,
		OrderRequirementsProjectionSubject: cfg.Kafka.RequirementsTopic,
		OrderDeliveryProjectionSubject:     cfg.Kafka.DeliveryTopic,
	}, lg)
}
