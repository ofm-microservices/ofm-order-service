package application

import (
	"context"
	"fmt"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	orderflowv1 "github.com/ofm-microservices/ofm-common/proto/orderflow/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"order-service/internal/domain"
)

type service struct {
	orders        OrderRepository
	read          OrderReadRepository
	broker        EventBroker
	resultSubject string
	log           Logger
}

// New constructs the order application service.
func New(orders OrderRepository, read OrderReadRepository, broker EventBroker, cfg Config, log Logger) (Service, error) {
	if orders == nil {
		return nil, ErrNilWriteRepository
	}
	if read == nil {
		return nil, ErrNilReadRepository
	}
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	resultSubject := cfg.OrderCreateResultSubject
	if resultSubject == "" {
		resultSubject = "order.create.result"
	}
	return &service{
		orders:        orders,
		read:          read,
		broker:        broker,
		resultSubject: resultSubject,
		log:           log.With(logging.String("module", "application")),
	}, nil
}

func (s *service) Create(ctx context.Context, cmd CreateOrderCommand) error {
	requestedAt := parseTimeOrNow(cmd.RequestedAt)
	order, err := s.orders.Create(ctx, domain.CreateOrderParams{
		OrderID:             cmd.OrderID,
		SagaID:              cmd.SagaID,
		BuyerID:             cmd.BuyerID,
		SellerID:            cmd.SellerID,
		SellerUsername:      cmd.SellerUsername,
		GigID:               cmd.GigID,
		GigTitle:            cmd.GigTitle,
		PackageID:           cmd.PackageID,
		PackageTier:         cmd.PackageTier,
		PackageDescription:  cmd.PackageDescription,
		PackageDeliveryDays: cmd.PackageDeliveryDays,
		PriceCents:          cmd.PriceCents,
		Currency:            cmd.Currency,
		IdempotencyKey:      cmd.IdempotencyKey,
		RequestedAt:         requestedAt,
	})
	if err != nil {
		return s.publishCreateResult(ctx, cmd.SagaID, cmd.OrderID, "failed", "order.create", err.Error())
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return err
	}
	return s.publishCreateResult(ctx, cmd.SagaID, cmd.OrderID, "success", "order.create", "")
}

func (s *service) Confirm(ctx context.Context, cmd ConfirmOrderCommand) error {
	order, err := s.orders.MarkFunded(ctx, cmd.OrderID, cmd.PaymentIntentID)
	if err != nil {
		return err
	}
	return s.read.Upsert(ctx, order)
}

func (s *service) Fail(ctx context.Context, cmd FailOrderCommand) error {
	order, err := s.orders.MarkFailed(ctx, cmd.OrderID, cmd.Reason)
	if err != nil {
		return err
	}
	return s.read.Upsert(ctx, order)
}

func (s *service) CreateDraftOrder(ctx context.Context, cmd CreateDraftOrderCommand) (*CreateDraftOrderResult, error) {
	order, err := s.orders.Create(ctx, domain.CreateOrderParams{
		OrderID:             cmd.OrderID,
		SagaID:              cmd.SagaID,
		BuyerID:             cmd.BuyerID,
		SellerID:            cmd.SellerID,
		SellerUsername:      cmd.SellerUsername,
		GigID:               cmd.GigID,
		GigTitle:            cmd.GigTitle,
		PackageID:           cmd.PackageID,
		PackageTier:         cmd.PackageTier,
		PackageDescription:  cmd.PackageDescription,
		PackageDeliveryDays: cmd.PackageDeliveryDays,
		PriceCents:          cmd.PriceCents,
		Currency:            cmd.Currency,
		IdempotencyKey:      cmd.IdempotencyKey,
		RequestedAt:         parseTimeOrNow(cmd.RequestedAt),
	})
	if err != nil {
		return nil, err
	}
	questions := make([]domain.OrderQuestionSnapshot, 0, len(cmd.Questions))
	for _, q := range cmd.Questions {
		questions = append(questions, domain.OrderQuestionSnapshot{
			OrderID:     order.OrderID,
			QuestionID:  q.QuestionID,
			Text:        q.Text,
			Type:        q.Type,
			Required:    q.Required,
			OptionsJSON: q.OptionsJSON,
			SortOrder:   q.SortOrder,
		})
	}
	if err := s.orders.SaveQuestionSnapshots(ctx, domain.SaveQuestionSnapshotsParams{OrderID: order.OrderID, Questions: questions}); err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &CreateDraftOrderResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*OrderPaymentSnapshot, error) {
	order, err := s.orders.GetByID(ctx, orderID)
	if err != nil {
		return nil, err
	}
	return &OrderPaymentSnapshot{
		OrderID:        order.OrderID,
		SagaID:         order.SagaID,
		BuyerID:        order.BuyerID,
		SellerID:       order.SellerID,
		SellerUsername: order.SellerUsername,
		GigTitle:       order.GigTitle,
		PackageTitle:   order.PackageTier,
		PriceCents:     order.PriceCents,
		Currency:       order.Currency,
		Status:         order.Status,
	}, nil
}

func (s *service) MarkPaymentPending(ctx context.Context, cmd MarkPaymentPendingCommand) (*MarkPaymentPendingResult, error) {
	if err := s.orders.SaveCheckoutSession(ctx, cmd.OrderID, cmd.PaymentIntentID, cmd.CheckoutURL); err != nil {
		return nil, err
	}
	order, err := s.orders.GetByID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkPaymentPendingResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) MarkOrderFunded(ctx context.Context, cmd MarkOrderFundedCommand) (*MarkOrderFundedResult, error) {
	order, err := s.orders.MarkFunded(ctx, cmd.OrderID, cmd.PaymentIntentID)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkOrderFundedResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) MarkPaymentFailed(ctx context.Context, cmd MarkPaymentFailedCommand) (*MarkPaymentFailedResult, error) {
	order, err := s.orders.MarkFailed(ctx, cmd.OrderID, cmd.Reason)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkPaymentFailedResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) SaveRequirementAnswers(ctx context.Context, cmd SaveRequirementAnswersCommand) (*SaveRequirementAnswersResult, error) {
	for _, ans := range cmd.Answers {
		if _, err := s.orders.SaveRequirementAnswers(ctx, domain.SaveRequirementAnswersParams{
			OrderID: cmd.OrderID,
			Answer:  domain.OrderAnswer{QuestionID: ans.QuestionID, Value: ans.Value},
		}); err != nil {
			return nil, err
		}
	}
	order, err := s.orders.GetByID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &SaveRequirementAnswersResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) SaveBuyerInitialMessage(ctx context.Context, cmd SaveBuyerInitialMessageCommand) (*SaveBuyerInitialMessageResult, error) {
	order, err := s.orders.SaveBuyerInitialMessage(ctx, domain.SaveBuyerInitialMessageParams{OrderID: cmd.OrderID, Message: cmd.Message})
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &SaveBuyerInitialMessageResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) AttachFile(ctx context.Context, cmd AttachFileCommand) (*AttachFileResult, error) {
	order, err := s.orders.AttachFile(ctx, domain.AttachFileParams{OrderID: cmd.OrderID, AttachmentID: cmd.AttachmentID})
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &AttachFileResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) GetOrderLifecycleSnapshot(ctx context.Context, orderID string) (*OrderLifecycleSnapshot, error) {
	order, err := s.orders.GetLifecycleSnapshot(ctx, orderID)
	if err != nil {
		return nil, err
	}
	return &OrderLifecycleSnapshot{
		OrderID:               order.OrderID,
		SagaID:                order.SagaID,
		BuyerID:               order.BuyerID,
		SellerID:              order.SellerID,
		SellerUsername:        order.SellerUsername,
		GigID:                 order.GigID,
		GigTitle:              order.GigTitle,
		PackageID:             order.PackageID,
		PackageTitle:          order.PackageTier,
		PackageDescription:    order.PackageDescription,
		PriceCents:            order.PriceCents,
		Currency:              order.Currency,
		Status:                order.Status,
		RevisionCountSnapshot: 0,
		RevisionCountUsed:     order.RevisionCountUsed,
		BuyerResponseDeadline: order.BuyerResponseDeadline.Format(time.RFC3339Nano),
		PaymentIntentID:       order.PaymentIntentID,
		PaymentReleaseID:      order.PaymentReleaseID,
		DeliveredAt:           order.DeliveredAt.Format(time.RFC3339Nano),
		CompletedAt:           order.CompletedAt.Format(time.RFC3339Nano),
		DisputedAt:            order.DisputedAt.Format(time.RFC3339Nano),
	}, nil
}

func (s *service) SaveDelivery(ctx context.Context, cmd SaveDeliveryCommand) (*SaveDeliveryResult, error) {
	order, err := s.orders.SaveDelivery(ctx, domain.SaveDeliveryParams{
		OrderID:       cmd.OrderID,
		SellerID:      cmd.SellerID,
		Message:       cmd.Message,
		AttachmentIDs: cmd.AttachmentIDs,
		RequestedAt:   parseTimeOrNow(cmd.RequestedAt),
	})
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &SaveDeliveryResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) MarkReleasePending(ctx context.Context, cmd MarkReleasePendingCommand) (*MarkReleasePendingResult, error) {
	order, err := s.orders.MarkReleasePending(ctx, cmd.OrderID, cmd.PaymentReleaseID)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkReleasePendingResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) RequestRevision(ctx context.Context, cmd RequestRevisionCommand) (*RequestRevisionResult, error) {
	order, err := s.orders.RequestRevision(ctx, domain.RequestRevisionParams{
		OrderID:     cmd.OrderID,
		BuyerID:     cmd.BuyerID,
		Reason:      cmd.Reason,
		RequestedAt: parseTimeOrNow(cmd.RequestedAt),
	})
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &RequestRevisionResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) OpenDispute(ctx context.Context, cmd OpenDisputeCommand) (*OpenDisputeResult, error) {
	order, err := s.orders.OpenDispute(ctx, domain.OpenDisputeParams{
		OrderID:     cmd.OrderID,
		BuyerID:     cmd.BuyerID,
		Reason:      cmd.Reason,
		RequestedAt: parseTimeOrNow(cmd.RequestedAt),
	})
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &OpenDisputeResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) MarkOrderCompleted(ctx context.Context, cmd MarkOrderCompletedCommand) (*MarkOrderCompletedResult, error) {
	order, err := s.orders.MarkCompleted(ctx, cmd.OrderID, cmd.PaymentReleaseID)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkOrderCompletedResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) MarkReleaseFailed(ctx context.Context, cmd MarkReleaseFailedCommand) (*MarkReleaseFailedResult, error) {
	order, err := s.orders.MarkReleaseFailed(ctx, cmd.OrderID, cmd.Reason)
	if err != nil {
		return nil, err
	}
	if err := s.read.Upsert(ctx, order); err != nil {
		return nil, err
	}
	return &MarkReleaseFailedResult{OrderID: order.OrderID, Status: order.Status}, nil
}

func (s *service) publishCreateResult(ctx context.Context, sagaID, orderID, status, operation, reason string) error {
	payload, err := protojson.Marshal(&orderflowv1.OrderSagaResult{
		SagaId:     sagaID,
		OrderId:    orderID,
		Status:     status,
		Error:      reason,
		Operation:  operation,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	if err := s.broker.Publish(ctx, s.resultSubject, payload); err != nil {
		return fmt.Errorf("%w: %v", ErrPublishResult, err)
	}
	return nil
}

func parseTimeOrNow(value string) time.Time {
	if value == "" {
		return time.Now().UTC()
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Now().UTC()
	}
	return parsed.UTC()
}
