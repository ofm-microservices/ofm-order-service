package nats

import (
	"context"
	"testing"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/config"
	app "order-service/internal/application"
	"order-service/internal/domain"
)

type deliveryReadStub struct{}

func (deliveryReadStub) Upsert(context.Context, *domain.Order) error { return nil }
func (deliveryReadStub) GetByID(context.Context, string) (*domain.Order, error) {
	return nil, nil
}
func (deliveryReadStub) GetPreviewByID(context.Context, string) (*domain.OrderPreview, error) {
	return nil, nil
}
func (deliveryReadStub) UpsertRequirements(context.Context, string, *domain.OrderRequirements) error {
	return nil
}
func (deliveryReadStub) UpsertDelivery(context.Context, string, *domain.OrderDeliveryProjection) error {
	return nil
}
func (deliveryReadStub) GetDeliveryByID(context.Context, string) (*domain.OrderDeliveryProjection, error) {
	return nil, nil
}
func (deliveryReadStub) GetRequirementsByID(context.Context, string) (*domain.OrderRequirements, error) {
	return nil, nil
}

type deliveryLoggerStub struct{}

func (deliveryLoggerStub) Debug(string, ...logging.Field)       {}
func (deliveryLoggerStub) Info(string, ...logging.Field)        {}
func (deliveryLoggerStub) Warn(string, ...logging.Field)        {}
func (deliveryLoggerStub) Error(string, ...logging.Field)       {}
func (deliveryLoggerStub) With(...logging.Field) logging.Logger { return deliveryLoggerStub{} }
func (deliveryLoggerStub) Sync() error                          { return nil }

func TestOrderDeliveryProjectionSubscriberSubscribeWiresSubject(t *testing.T) {
	broker := &brokerRecord{}
	svc := &commandService{}
	var _ app.OrderReadRepository = deliveryReadStub{}
	subscriber, err := NewOrderDeliveryProjectionSubscriber(broker, svc, deliveryReadStub{}, config.NATSConfig{
		OrderDeliveryProjectionSubject: "order.projection.delivery",
		OrderDeliveryProjectionDurable: "delivery-durable",
	}, deliveryLoggerStub{})
	if err != nil {
		t.Fatalf("NewOrderDeliveryProjectionSubscriber: %v", err)
	}
	if err := subscriber.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(broker.subjects) != 1 || broker.subjects[0] != "order.projection.delivery" {
		t.Fatalf("subjects = %#v", broker.subjects)
	}
}
