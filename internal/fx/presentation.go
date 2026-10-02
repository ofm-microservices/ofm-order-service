package appfx

import (
	"context"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	app "order-service/internal/application"
	eventbroker "order-service/internal/presentation/event_broker"
	events "order-service/internal/presentation/event_broker/kafka"
	grpcsrv "order-service/internal/presentation/grpc"
)

// PresentationModule wires Kafka subscribers into the FX lifecycle.
var PresentationModule = fx.Options(
	fx.Provide(ProvideOrderCommandSubscriber),
	fx.Provide(ProvideOrderPreviewProjectionSubscriber),
	fx.Provide(ProvideOrderRequirementsProjectionSubscriber),
	fx.Provide(ProvideOrderDeliveryProjectionSubscriber),
	fx.Provide(ProvideOrderRecoverySubscriber),
	fx.Provide(ProvideGRPCServer),
	fx.Invoke(InvokeSubscribeOrderCommands),
	fx.Invoke(InvokeSubscribeOrderPreviewProjection),
	fx.Invoke(InvokeSubscribeOrderRequirementsProjection),
	fx.Invoke(InvokeSubscribeOrderDeliveryProjection),
	fx.Invoke(InvokeSubscribeOrderRecovery),
	fx.Invoke(InvokeRunGRPCServer),
)

// ProvideOrderRecoverySubscriber constructs the order-owned recovery consumer.
func ProvideOrderRecoverySubscriber(broker eventbroker.EventBroker, service app.Service, cfg *config.Config, lg logging.Logger) (events.RecoverySubscriber, error) {
	return events.NewRecoverySubscriber(broker, service, cfg.Kafka, lg)
}

// InvokeSubscribeOrderRecovery starts the order recovery consumer.
func InvokeSubscribeOrderRecovery(lc fx.Lifecycle, subscriber events.RecoverySubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		ctx, stop := context.WithCancel(context.Background())
		cancel = stop
		go func() {
			for ctx.Err() == nil {
				if err := subscriber.Subscribe(ctx); err != nil && ctx.Err() == nil {
					lg.Error("order recovery consumer stopped; retrying", logging.Err(err))
					timer := time.NewTimer(time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}()
		return nil
	}, OnStop: func(context.Context) error {
		if cancel != nil {
			cancel()
		}
		return nil
	}})
}

// ProvideOrderCommandSubscriber constructs the order command subscriber.
func ProvideOrderCommandSubscriber(broker eventbroker.EventBroker, service app.Service, cfg *config.Config) (events.OrderCommandSubscriber, error) {
	return events.NewOrderCommandSubscriber(broker, service, cfg.Kafka)
}

// ProvideOrderPreviewProjectionSubscriber constructs the order preview projection consumer.
func ProvideOrderPreviewProjectionSubscriber(broker eventbroker.EventBroker, readRepo app.OrderReadRepository, cfg *config.Config, lg logging.Logger) (events.OrderPreviewProjectionSubscriber, error) {
	return events.NewOrderPreviewProjectionSubscriber(broker, readRepo, cfg.Kafka, lg)
}

// ProvideOrderRequirementsProjectionSubscriber constructs the order requirements projection consumer.
func ProvideOrderRequirementsProjectionSubscriber(broker eventbroker.EventBroker, readRepo app.OrderReadRepository, cfg *config.Config, lg logging.Logger) (events.OrderRequirementsProjectionSubscriber, error) {
	return events.NewOrderRequirementsProjectionSubscriber(broker, readRepo, cfg.Kafka, lg)
}

// ProvideOrderDeliveryProjectionSubscriber constructs the order delivery projection consumer.
func ProvideOrderDeliveryProjectionSubscriber(broker eventbroker.EventBroker, service app.Service, readRepo app.OrderReadRepository, cfg *config.Config, lg logging.Logger) (events.OrderDeliveryProjectionSubscriber, error) {
	return events.NewOrderDeliveryProjectionSubscriber(broker, service, readRepo, cfg.Kafka, lg)
}

// InvokeSubscribeOrderCommands starts the order command consumers with the FX
// lifecycle.
func InvokeSubscribeOrderCommands(lc fx.Lifecycle, subscriber events.OrderCommandSubscriber, cfg *config.Config, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := subscriber.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe to order commands failed", logging.Err(err))
				}
			}()
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
			go func() {
				if err := subscriber.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe to order preview projection failed", logging.Err(err))
				}
			}()
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

// InvokeSubscribeOrderRequirementsProjection starts the order requirements projection consumer.
func InvokeSubscribeOrderRequirementsProjection(lc fx.Lifecycle, subscriber events.OrderRequirementsProjectionSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := subscriber.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe to order requirements projection failed", logging.Err(err))
				}
			}()
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

// InvokeSubscribeOrderDeliveryProjection starts the order delivery projection consumer.
func InvokeSubscribeOrderDeliveryProjection(lc fx.Lifecycle, subscriber events.OrderDeliveryProjectionSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := subscriber.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe to order delivery projection failed", logging.Err(err))
				}
			}()
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
