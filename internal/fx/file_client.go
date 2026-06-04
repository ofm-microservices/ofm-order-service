package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	filegrpc "order-service/internal/infra/file/grpc"
)

// FileClientModule wires the upstream file-service client into order-service.
var FileClientModule = fx.Options(fx.Provide(ProvideFileServiceClient))

// ProvideFileServiceClient constructs the upstream file-service gRPC client.
func ProvideFileServiceClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (filegrpc.FileService, error) {
	client, err := filegrpc.New(cfg.File, lg)
	if err != nil {
		lg.Error("connect file service failed", logging.Err(err))
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})
	return client, nil
}
