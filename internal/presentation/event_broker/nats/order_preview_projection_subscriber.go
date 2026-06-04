package nats

import (
	"context"
	"encoding/json"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
)

// OrderPreviewProjectionSubscriber consumes order preview projection jobs from JetStream.
type OrderPreviewProjectionSubscriber interface {
	Subscribe(ctx context.Context) error
}

type orderPreviewProjectionSubscriber struct {
	broker app.EventBroker
	read   app.OrderReadRepository
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewOrderPreviewProjectionSubscriber constructs the preview projection subscriber.
func NewOrderPreviewProjectionSubscriber(broker app.EventBroker, read app.OrderReadRepository, cfg config.NATSConfig, log logging.Logger) (OrderPreviewProjectionSubscriber, error) {
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if read == nil {
		return nil, ErrNilOrderReadRepository
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &orderPreviewProjectionSubscriber{
		broker: broker,
		read:   read,
		cfg:    cfg,
		log:    log.With(logging.String("module", "order-preview-projection-subscriber")),
	}, nil
}

func (s *orderPreviewProjectionSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.cfg.OrderPreviewProjectionSubject, s.cfg.OrderPreviewProjectionDurable, s.handle)
}

func (s *orderPreviewProjectionSubscriber) handle(ctx context.Context, _ string, payload []byte) error {
	var order domain.Order
	if err := json.Unmarshal(payload, &order); err != nil {
		return err
	}
	return s.read.Upsert(ctx, &order)
}
