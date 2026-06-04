package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	usergrpc "order-service/internal/infra/user/grpc"
)

// UserClientModule wires the upstream user-service client into order-service.
var UserClientModule = fx.Options(fx.Provide(ProvideUserServiceClient))

// ProvideUserServiceClient constructs the upstream user-service gRPC client.
func ProvideUserServiceClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (usergrpc.UserService, error) {
	client, err := usergrpc.New(cfg.User, lg)
	if err != nil {
		lg.Error("connect user service failed", logging.Err(err))
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})
	return client, nil
}
