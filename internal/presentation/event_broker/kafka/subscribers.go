package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
)

type subscriber interface{ Subscribe(context.Context) error }

// OrderCommandSubscriber consumes order commands from Kafka.
type OrderCommandSubscriber interface{ Subscribe(context.Context) error }

// OrderPreviewProjectionSubscriber consumes preview projection requests from Kafka.
type OrderPreviewProjectionSubscriber interface{ Subscribe(context.Context) error }

// OrderRequirementsProjectionSubscriber consumes requirements projection requests from Kafka.
type OrderRequirementsProjectionSubscriber interface{ Subscribe(context.Context) error }

// OrderDeliveryProjectionSubscriber consumes delivery projection requests from Kafka.
type OrderDeliveryProjectionSubscriber interface{ Subscribe(context.Context) error }

type commandSubscriber struct {
	broker  app.EventBroker
	service app.Service
	cfg     config.KafkaConfig
}

// NewOrderCommandSubscriber constructs the Kafka order command consumer.
func NewOrderCommandSubscriber(broker app.EventBroker, service app.Service, cfg config.KafkaConfig) (OrderCommandSubscriber, error) {
	if broker == nil {
		return nil, errors.New("event broker is nil")
	}
	if service == nil {
		return nil, errors.New("order service is nil")
	}
	return commandSubscriber{broker: broker, service: service, cfg: cfg}, nil
}
func (s commandSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.Subscribe(ctx, s.cfg.CreateTopic, "", s.create); err != nil {
		return err
	}
	if err := s.broker.Subscribe(ctx, s.cfg.ConfirmTopic, "", s.confirm); err != nil {
		return err
	}
	return s.broker.Subscribe(ctx, s.cfg.FailTopic, "", s.fail)
}
func (s commandSubscriber) create(ctx context.Context, _ string, p []byte) error {
	var c orderflowv1.OrderCreateCommand
	if err := protojson.Unmarshal(p, &c); err != nil {
		return err
	}
	return s.service.Create(ctx, app.CreateOrderCommand{SagaID: c.GetSagaId(), OrderID: c.GetOrderId(), BuyerID: c.GetBuyerId(), SellerID: c.GetSellerId(), SellerUsername: c.GetSellerUsername(), GigID: c.GetGigId(), GigTitle: c.GetGigTitle(), PictureFileID: c.GetPictureFileId(), PackageID: c.GetPackageId(), PackageTier: c.GetPackageTier(), PackageDescription: c.GetPackageDescription(), PackageDeliveryDays: c.GetPackageDeliveryDays(), PriceCents: c.GetPriceCents(), Currency: c.GetCurrency(), IdempotencyKey: c.GetIdempotencyKey(), RequestedAt: c.GetRequestedAt()})
}
func (s commandSubscriber) confirm(ctx context.Context, _ string, p []byte) error {
	var c orderflowv1.OrderConfirmedEvent
	if err := protojson.Unmarshal(p, &c); err != nil {
		return err
	}
	return s.service.Confirm(ctx, app.ConfirmOrderCommand{SagaID: c.GetSagaId(), OrderID: c.GetOrderId(), PaymentIntentID: c.GetPaymentIntentId(), OccurredAt: c.GetOccurredAt()})
}
func (s commandSubscriber) fail(ctx context.Context, _ string, p []byte) error {
	var c orderflowv1.OrderFailedEvent
	if err := protojson.Unmarshal(p, &c); err != nil {
		return err
	}
	return s.service.Fail(ctx, app.FailOrderCommand{SagaID: c.GetSagaId(), OrderID: c.GetOrderId(), Reason: c.GetReason(), OccurredAt: c.GetOccurredAt()})
}

type previewSubscriber struct {
	broker app.EventBroker
	read   app.OrderReadRepository
	topic  string
	log    logging.Logger
}

// NewOrderPreviewProjectionSubscriber constructs the Kafka preview projection consumer.
func NewOrderPreviewProjectionSubscriber(b app.EventBroker, r app.OrderReadRepository, c config.KafkaConfig, l logging.Logger) (OrderPreviewProjectionSubscriber, error) {
	if b == nil || r == nil || l == nil {
		return nil, errors.New("invalid preview subscriber dependency")
	}
	return previewSubscriber{broker: b, read: r, topic: c.PreviewTopic, log: l}, nil
}
func (s previewSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.topic, "", s.handle)
}
func (s previewSubscriber) handle(ctx context.Context, _ string, p []byte) error {
	var o domain.Order
	if err := json.Unmarshal(p, &o); err != nil {
		return err
	}
	return s.read.Upsert(ctx, &o)
}

type requirementsSubscriber struct {
	broker app.EventBroker
	read   app.OrderReadRepository
	topic  string
	log    logging.Logger
}

// NewOrderRequirementsProjectionSubscriber constructs the Kafka requirements projection consumer.
func NewOrderRequirementsProjectionSubscriber(b app.EventBroker, r app.OrderReadRepository, c config.KafkaConfig, l logging.Logger) (OrderRequirementsProjectionSubscriber, error) {
	if b == nil || r == nil || l == nil {
		return nil, errors.New("invalid requirements subscriber dependency")
	}
	return requirementsSubscriber{broker: b, read: r, topic: c.RequirementsTopic, log: l}, nil
}
func (s requirementsSubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.topic, "", s.handle)
}
func (s requirementsSubscriber) handle(ctx context.Context, _ string, p []byte) error {
	var r domain.OrderRequirements
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	return s.read.UpsertRequirements(ctx, r.OrderID, &r)
}

type deliverySubscriber struct {
	broker  app.EventBroker
	service app.Service
	read    app.OrderReadRepository
	topic   string
	log     logging.Logger
}

// NewOrderDeliveryProjectionSubscriber constructs the Kafka delivery projection consumer.
func NewOrderDeliveryProjectionSubscriber(b app.EventBroker, s app.Service, r app.OrderReadRepository, c config.KafkaConfig, l logging.Logger) (OrderDeliveryProjectionSubscriber, error) {
	if b == nil || s == nil || r == nil || l == nil {
		return nil, errors.New("invalid delivery subscriber dependency")
	}
	return deliverySubscriber{broker: b, service: s, read: r, topic: c.DeliveryTopic, log: l}, nil
}
func (s deliverySubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.topic, "", s.handle)
}
func (s deliverySubscriber) handle(ctx context.Context, _ string, p []byte) error {
	var q app.OrderDeliveryProjectionRequest
	if err := json.Unmarshal(p, &q); err != nil {
		return err
	}
	x, e := s.service.BuildOrderDeliveryProjection(ctx, q.OrderID)
	if e != nil {
		return e
	}
	d := deliveryProjection(q.OrderID, x)
	if d == nil {
		return domain.ErrOrderNotFound
	}
	return s.read.UpsertDelivery(ctx, q.OrderID, d)
}
func deliveryProjection(id string, p *app.OrderDeliveryProjection) *domain.OrderDeliveryProjection {
	if p == nil || p.OrderDelivery == nil {
		return nil
	}
	o := &domain.OrderDeliveryProjection{OrderID: id, Delivery: &domain.OrderDelivery{OrderID: id, DeliveryMessage: strings.TrimSpace(p.OrderDelivery.DeliveryMessage)}, DeliveryFiles: make([]domain.OrderDeliveryFile, 0, len(p.OrderDeliveryFiles))}
	if t, e := time.Parse(time.RFC3339Nano, p.OrderDelivery.CreatedAt); e == nil {
		o.Delivery.CreatedAt = t
	}
	for _, f := range p.OrderDeliveryFiles {
		t, _ := time.Parse(time.RFC3339Nano, f.CreatedAt)
		o.DeliveryFiles = append(o.DeliveryFiles, domain.OrderDeliveryFile{OrderID: id, FileID: strings.TrimSpace(f.FileID), FileURL: strings.TrimSpace(f.FileURL), SortOrder: f.SortOrder, CreatedAt: t})
	}
	return o
}
