package nats

import (
	"context"
	"encoding/json"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
)

// OrderRequirementsProjectionSubscriber consumes order requirements projection jobs from JetStream.
type OrderRequirementsProjectionSubscriber interface {
	Subscribe(ctx context.Context) error
}

type orderRequirementsProjectionSubscriber struct {
	broker app.EventBroker
	read   app.OrderReadRepository
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewOrderRequirementsProjectionSubscriber constructs the requirements projection subscriber.
func NewOrderRequirementsProjectionSubscriber(broker app.EventBroker, read app.OrderReadRepository, cfg config.NATSConfig, log logging.Logger) (OrderRequirementsProjectionSubscriber, error) {
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if read == nil {
		return nil, ErrNilOrderReadRepository
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &orderRequirementsProjectionSubscriber{
		broker: broker,
		read:   read,
		cfg:    cfg,
		log:    log.With(logging.String("module", "order-requirements-projection-subscriber")),
	}, nil
}

func (s *orderRequirementsProjectionSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.cfg.OrderRequirementsProjectionSubject, s.cfg.OrderRequirementsProjectionDurable, s.handle)
}

func (s *orderRequirementsProjectionSubscriber) handle(ctx context.Context, _ string, payload []byte) error {
	var reqs domain.OrderRequirements
	if err := json.Unmarshal(payload, &reqs); err != nil {
		return err
	}
	return s.read.UpsertRequirements(ctx, reqs.OrderID, &reqs)
}
