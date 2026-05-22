package nats

import (
	"context"

	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-service/config"
	app "order-service/internal/application"
)

// OrderCommandSubscriber consumes order saga commands from JetStream.
type OrderCommandSubscriber interface {
	Subscribe(ctx context.Context) error
}

type orderCommandSubscriber struct {
	broker  app.EventBroker
	service app.Service
	cfg     config.NATSConfig
}

// NewOrderCommandSubscriber constructs the order command subscriber.
func NewOrderCommandSubscriber(broker app.EventBroker, service app.Service, cfg config.NATSConfig) (OrderCommandSubscriber, error) {
	return &orderCommandSubscriber{broker: broker, service: service, cfg: cfg}, nil
}

func (s *orderCommandSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.Subscribe(ctx, s.cfg.OrderCreateSubject, s.cfg.OrderCreateDurable, s.handleCreate); err != nil {
		return err
	}
	if err := s.broker.Subscribe(ctx, s.cfg.OrderConfirmSubject, s.cfg.OrderConfirmDurable, s.handleConfirm); err != nil {
		return err
	}
	return s.broker.Subscribe(ctx, s.cfg.OrderFailSubject, s.cfg.OrderFailDurable, s.handleFail)
}

func (s *orderCommandSubscriber) handleCreate(ctx context.Context, _ string, payload []byte) error {
	var cmd orderflowv1.OrderCreateCommand
	if err := protojson.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return s.service.Create(ctx, app.CreateOrderCommand{
		SagaID:              cmd.GetSagaId(),
		OrderID:             cmd.GetOrderId(),
		BuyerID:             cmd.GetBuyerId(),
		SellerID:            cmd.GetSellerId(),
		GigID:               cmd.GetGigId(),
		GigTitle:            cmd.GetGigTitle(),
		PackageID:           cmd.GetPackageId(),
		PackageTier:         cmd.GetPackageTier(),
		PackageDescription:  cmd.GetPackageDescription(),
		PackageDeliveryDays: cmd.GetPackageDeliveryDays(),
		PriceCents:          cmd.GetPriceCents(),
		Currency:            cmd.GetCurrency(),
		IdempotencyKey:      cmd.GetIdempotencyKey(),
		RequestedAt:         cmd.GetRequestedAt(),
	})
}

func (s *orderCommandSubscriber) handleConfirm(ctx context.Context, _ string, payload []byte) error {
	var cmd orderflowv1.OrderConfirmedEvent
	if err := protojson.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return s.service.Confirm(ctx, app.ConfirmOrderCommand{
		SagaID:          cmd.GetSagaId(),
		OrderID:         cmd.GetOrderId(),
		PaymentIntentID: cmd.GetPaymentIntentId(),
		OccurredAt:      cmd.GetOccurredAt(),
	})
}

func (s *orderCommandSubscriber) handleFail(ctx context.Context, _ string, payload []byte) error {
	var cmd orderflowv1.OrderFailedEvent
	if err := protojson.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	return s.service.Fail(ctx, app.FailOrderCommand{
		SagaID:     cmd.GetSagaId(),
		OrderID:    cmd.GetOrderId(),
		Reason:     cmd.GetReason(),
		OccurredAt: cmd.GetOccurredAt(),
	})
}
