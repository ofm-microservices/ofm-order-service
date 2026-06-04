package nats

import (
	"context"
	"testing"

	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-service/config"
	app "order-service/internal/application"
)

type brokerRecord struct {
	subjects []string
	durables []string
	handlers []app.MessageHandler
}

func (b *brokerRecord) Publish(context.Context, string, []byte) error { return nil }
func (b *brokerRecord) Subscribe(_ context.Context, subject, durable string, handler app.MessageHandler) error {
	b.subjects = append(b.subjects, subject)
	b.durables = append(b.durables, durable)
	b.handlers = append(b.handlers, handler)
	return nil
}
func (b *brokerRecord) Close() {}

type commandService struct {
	createCmds  []app.CreateOrderCommand
	confirmCmds []app.ConfirmOrderCommand
	failCmds    []app.FailOrderCommand
}

func (s *commandService) Create(_ context.Context, cmd app.CreateOrderCommand) error {
	s.createCmds = append(s.createCmds, cmd)
	return nil
}
func (s *commandService) Confirm(_ context.Context, cmd app.ConfirmOrderCommand) error {
	s.confirmCmds = append(s.confirmCmds, cmd)
	return nil
}
func (s *commandService) Fail(_ context.Context, cmd app.FailOrderCommand) error {
	s.failCmds = append(s.failCmds, cmd)
	return nil
}
func (s *commandService) CreateDraftOrder(context.Context, app.CreateDraftOrderCommand) (*app.CreateDraftOrderResult, error) {
	return nil, nil
}
func (s *commandService) GetOrderPaymentSnapshot(context.Context, string) (*app.OrderPaymentSnapshot, error) {
	return nil, nil
}
func (s *commandService) GetOrderPreviewByID(context.Context, app.GetOrderPreviewByIDCommand) (*app.OrderPreviewResult, error) {
	return nil, nil
}
func (s *commandService) GetOrderRequirementsByID(context.Context, app.GetOrderRequirementsByIDCommand) (*app.OrderRequirementsResult, error) {
	return nil, nil
}
func (s *commandService) GetOrderDeliveryByID(context.Context, app.GetOrderDeliveryByIDCommand) (*app.OrderDeliveryProjection, error) {
	return nil, nil
}
func (s *commandService) MarkPaymentPending(context.Context, app.MarkPaymentPendingCommand) (*app.MarkPaymentPendingResult, error) {
	return nil, nil
}
func (s *commandService) MarkOrderFunded(context.Context, app.MarkOrderFundedCommand) (*app.MarkOrderFundedResult, error) {
	return nil, nil
}
func (s *commandService) MarkPaymentFailed(context.Context, app.MarkPaymentFailedCommand) (*app.MarkPaymentFailedResult, error) {
	return nil, nil
}
func (s *commandService) SaveRequirementAnswers(context.Context, app.SaveRequirementAnswersCommand) (*app.SaveRequirementAnswersResult, error) {
	return nil, nil
}
func (s *commandService) SaveBuyerInitialMessage(context.Context, app.SaveBuyerInitialMessageCommand) (*app.SaveBuyerInitialMessageResult, error) {
	return nil, nil
}
func (s *commandService) AttachFile(context.Context, app.AttachFileCommand) (*app.AttachFileResult, error) {
	return nil, nil
}
func (s *commandService) GetOrderLifecycleSnapshot(context.Context, string) (*app.OrderLifecycleSnapshot, error) {
	return nil, nil
}
func (s *commandService) SaveDelivery(context.Context, app.SaveDeliveryCommand) (*app.SaveDeliveryResult, error) {
	return nil, nil
}
func (s *commandService) BuildOrderDeliveryProjection(context.Context, string) (*app.OrderDeliveryProjection, error) {
	return nil, nil
}
func (s *commandService) MarkReleasePending(context.Context, app.MarkReleasePendingCommand) (*app.MarkReleasePendingResult, error) {
	return nil, nil
}
func (s *commandService) RequestRevision(context.Context, app.RequestRevisionCommand) (*app.RequestRevisionResult, error) {
	return nil, nil
}
func (s *commandService) OpenDispute(context.Context, app.OpenDisputeCommand) (*app.OpenDisputeResult, error) {
	return nil, nil
}
func (s *commandService) MarkOrderCompleted(context.Context, app.MarkOrderCompletedCommand) (*app.MarkOrderCompletedResult, error) {
	return nil, nil
}
func (s *commandService) MarkReleaseFailed(context.Context, app.MarkReleaseFailedCommand) (*app.MarkReleaseFailedResult, error) {
	return nil, nil
}

func TestOrderCommandSubscriberSubscribeWiresSubjects(t *testing.T) {
	broker := &brokerRecord{}
	svc := &commandService{}
	subscriber, err := NewOrderCommandSubscriber(broker, svc, config.NATSConfig{
		OrderCreateSubject:  "order.create",
		OrderConfirmSubject: "order.confirm",
		OrderFailSubject:    "order.fail",
		OrderCreateDurable:  "create-durable",
		OrderConfirmDurable: "confirm-durable",
		OrderFailDurable:    "fail-durable",
	})
	if err != nil {
		t.Fatalf("NewOrderCommandSubscriber: %v", err)
	}
	if err := subscriber.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(broker.subjects) != 3 {
		t.Fatalf("subjects = %#v", broker.subjects)
	}
}

func TestOrderCommandSubscriberHandlers(t *testing.T) {
	broker := &brokerRecord{}
	svc := &commandService{}
	subscriber, _ := NewOrderCommandSubscriber(broker, svc, config.NATSConfig{})

	createPayload, _ := protojson.Marshal(&orderflowv1.OrderCreateCommand{
		SagaId: "saga-1", OrderId: "order-1", BuyerId: "buyer-1", SellerId: "seller-1",
		GigId: "gig-1", GigTitle: "Gig", PackageId: "pkg-1", PackageTier: "Basic",
		PackageDescription: "desc", PackageDeliveryDays: 3, PriceCents: 1000, Currency: "usd",
		IdempotencyKey: "idem-1", RequestedAt: "2026-05-24T00:00:00Z",
	})
	if err := subscriber.(*orderCommandSubscriber).handleCreate(context.Background(), "order.create", createPayload); err != nil {
		t.Fatalf("handleCreate: %v", err)
	}
	if len(svc.createCmds) != 1 || svc.createCmds[0].OrderID != "order-1" {
		t.Fatalf("createCmds = %#v", svc.createCmds)
	}

	confirmPayload, _ := protojson.Marshal(&orderflowv1.OrderConfirmedEvent{
		SagaId: "saga-1", OrderId: "order-1", PaymentIntentId: "pi-1", OccurredAt: "2026-05-24T00:00:00Z",
	})
	if err := subscriber.(*orderCommandSubscriber).handleConfirm(context.Background(), "order.confirm", confirmPayload); err != nil {
		t.Fatalf("handleConfirm: %v", err)
	}
	if len(svc.confirmCmds) != 1 || svc.confirmCmds[0].PaymentIntentID != "pi-1" {
		t.Fatalf("confirmCmds = %#v", svc.confirmCmds)
	}

	failPayload, _ := protojson.Marshal(&orderflowv1.OrderFailedEvent{
		SagaId: "saga-1", OrderId: "order-1", Reason: "boom", OccurredAt: "2026-05-24T00:00:00Z",
	})
	if err := subscriber.(*orderCommandSubscriber).handleFail(context.Background(), "order.fail", failPayload); err != nil {
		t.Fatalf("handleFail: %v", err)
	}
	if len(svc.failCmds) != 1 || svc.failCmds[0].Reason != "boom" {
		t.Fatalf("failCmds = %#v", svc.failCmds)
	}
}

var _ app.Service = (*commandService)(nil)
var _ app.EventBroker = (*brokerRecord)(nil)
