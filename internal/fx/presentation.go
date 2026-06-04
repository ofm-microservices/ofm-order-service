package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	app "order-service/internal/application"
	eventbroker "order-service/internal/presentation/event_broker"
	events "order-service/internal/presentation/event_broker/nats"
	grpcsrv "order-service/internal/presentation/grpc"
)

// PresentationModule wires JetStream subscribers into the FX lifecycle.
var PresentationModule = fx.Options(
	fx.Provide(ProvideOrderCommandSubscriber),
	fx.Provide(ProvideOrderPreviewProjectionSubscriber),
	fx.Provide(ProvideGRPCServer),
	fx.Invoke(InvokeSubscribeOrderCommands),
	fx.Invoke(InvokeSubscribeOrderPreviewProjection),
	fx.Invoke(InvokeRunGRPCServer),
)

// ProvideOrderCommandSubscriber constructs the order command subscriber.
func ProvideOrderCommandSubscriber(broker eventbroker.EventBroker, service app.Service, cfg *config.Config) (events.OrderCommandSubscriber, error) {
	return events.NewOrderCommandSubscriber(broker, service, cfg.NATS)
}

// ProvideOrderPreviewProjectionSubscriber constructs the order preview projection consumer.
func ProvideOrderPreviewProjectionSubscriber(broker eventbroker.EventBroker, readRepo app.OrderReadRepository, cfg *config.Config, lg logging.Logger) (events.OrderPreviewProjectionSubscriber, error) {
	return events.NewOrderPreviewProjectionSubscriber(broker, readRepo, cfg.NATS, lg)
}

// InvokeSubscribeOrderCommands starts the order command consumers with the FX
// lifecycle.
func InvokeSubscribeOrderCommands(lc fx.Lifecycle, subscriber events.OrderCommandSubscriber, cfg *config.Config, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			if err := subscriber.Subscribe(runCtx); err != nil {
				lg.Error("subscribe to order commands failed", logging.Err(err))
				cancel()
				return err
			}
			lg.Info("order-service initialized", logging.String("env", cfg.App.Env))
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// InvokeSubscribeOrderPreviewProjection starts the order preview projection consumer.
func InvokeSubscribeOrderPreviewProjection(lc fx.Lifecycle, subscriber events.OrderPreviewProjectionSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			if err := subscriber.Subscribe(runCtx); err != nil {
				lg.Error("subscribe to order preview projection failed", logging.Err(err))
				cancel()
				return err
			}
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

func ProvideGRPCServer(service app.Service, cfg *config.Config, lg logging.Logger) (grpcsrv.Server, error) {
	return grpcsrv.NewServer(service, cfg.GRPC, lg)
}

func InvokeRunGRPCServer(lc fx.Lifecycle, srv grpcsrv.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { go func() { _ = srv.Start() }(); return nil },
		OnStop:  func(ctx context.Context) error { return srv.Shutdown(ctx) },
	})
}
