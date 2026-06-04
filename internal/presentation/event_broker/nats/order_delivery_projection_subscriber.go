package nats

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
)

// OrderDeliveryProjectionSubscriber consumes delivery projection jobs from JetStream.
type OrderDeliveryProjectionSubscriber interface {
	Subscribe(ctx context.Context) error
}

type orderDeliveryProjectionSubscriber struct {
	broker  app.EventBroker
	service app.Service
	read    app.OrderReadRepository
	cfg     config.NATSConfig
	log     logging.Logger
}

// NewOrderDeliveryProjectionSubscriber constructs the delivery projection subscriber.
func NewOrderDeliveryProjectionSubscriber(broker app.EventBroker, service app.Service, read app.OrderReadRepository, cfg config.NATSConfig, log logging.Logger) (OrderDeliveryProjectionSubscriber, error) {
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if service == nil {
		return nil, ErrNilOrderService
	}
	if read == nil {
		return nil, ErrNilOrderReadRepository
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &orderDeliveryProjectionSubscriber{
		broker:  broker,
		service: service,
		read:    read,
		cfg:     cfg,
		log:     log.With(logging.String("module", "order-delivery-projection-subscriber")),
	}, nil
}

func (s *orderDeliveryProjectionSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.cfg.OrderDeliveryProjectionSubject, s.cfg.OrderDeliveryProjectionDurable, s.handle)
}

func (s *orderDeliveryProjectionSubscriber) handle(ctx context.Context, _ string, payload []byte) error {
	var req app.OrderDeliveryProjectionRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}
	projection, err := s.service.BuildOrderDeliveryProjection(ctx, req.OrderID)
	if err != nil {
		return err
	}
	domainProjection := toDomainDeliveryProjection(req.OrderID, projection)
	if domainProjection == nil {
		return domain.ErrOrderNotFound
	}
	return s.read.UpsertDelivery(ctx, req.OrderID, domainProjection)
}

func toDomainDeliveryProjection(orderID string, projection *app.OrderDeliveryProjection) *domain.OrderDeliveryProjection {
	if projection == nil || projection.OrderDelivery == nil {
		return nil
	}
	out := &domain.OrderDeliveryProjection{
		OrderID: orderID,
		Delivery: &domain.OrderDelivery{
			OrderID:         orderID,
			DeliveryMessage: strings.TrimSpace(projection.OrderDelivery.DeliveryMessage),
		},
		DeliveryFiles: make([]domain.OrderDeliveryFile, 0, len(projection.OrderDeliveryFiles)),
	}
	if ts, err := time.Parse(time.RFC3339Nano, projection.OrderDelivery.CreatedAt); err == nil {
		out.Delivery.CreatedAt = ts
	}
	for _, file := range projection.OrderDeliveryFiles {
		createdAt, _ := time.Parse(time.RFC3339Nano, file.CreatedAt)
		out.DeliveryFiles = append(out.DeliveryFiles, domain.OrderDeliveryFile{
			OrderID:   orderID,
			FileID:    strings.TrimSpace(file.FileID),
			FileURL:   strings.TrimSpace(file.FileURL),
			SortOrder: file.SortOrder,
			CreatedAt: createdAt,
		})
	}
	return out
}
