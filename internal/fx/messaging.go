package appfx

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"order-service/config"
	eventbroker "order-service/internal/presentation/event_broker"
	broker "order-service/internal/presentation/event_broker/kafka"
)

// MessagingModule wires the Kafka event broker into order-service.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithDB),
)

// InvokeEnsureStream is retained as a compatibility no-op; Kafka topics are
// provisioned by the broker/runtime rather than JetStream bootstrap.
func InvokeEnsureStream(*config.Config, logging.Logger) error { return nil }

// ProvideEventBroker constructs the concrete Kafka event broker.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, nil, lg)
}

// ProvideEventBrokerWithDB enables durable event claims in production wiring.
func ProvideEventBrokerWithDB(lc fx.Lifecycle, cfg *config.Config, db *sqlx.DB, lg logging.Logger) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, db, lg)
}
func provideEventBroker(lc fx.Lifecycle, cfg *config.Config, db *sqlx.DB, lg logging.Logger) (eventbroker.EventBroker, error) {
	var eventBroker eventbroker.EventBroker
	var err error
	if db == nil {
		eventBroker, err = broker.NewBroker(cfg.Kafka)
	} else {
		eventBroker, err = broker.NewBrokerWithDB(cfg.Kafka, db)
	}
	if err != nil {
		lg.Error("connect kafka failed", logging.Err(err))
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { eventBroker.Close(); return nil }})
	return eventBroker, nil
}
