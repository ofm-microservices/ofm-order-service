package application

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-service/internal/domain"
)

// Service owns order command handling for the order saga.
type Service interface {
	Create(ctx context.Context, cmd CreateOrderCommand) error
	Confirm(ctx context.Context, cmd ConfirmOrderCommand) error
	Fail(ctx context.Context, cmd FailOrderCommand) error
	CreateDraftOrder(ctx context.Context, cmd CreateDraftOrderCommand) (*CreateDraftOrderResult, error)
	GetOrderPaymentSnapshot(ctx context.Context, orderID string) (*OrderPaymentSnapshot, error)
	MarkPaymentPending(ctx context.Context, cmd MarkPaymentPendingCommand) (*MarkPaymentPendingResult, error)
	MarkOrderFunded(ctx context.Context, cmd MarkOrderFundedCommand) (*MarkOrderFundedResult, error)
	MarkPaymentFailed(ctx context.Context, cmd MarkPaymentFailedCommand) (*MarkPaymentFailedResult, error)
	SaveRequirementAnswers(ctx context.Context, cmd SaveRequirementAnswersCommand) (*SaveRequirementAnswersResult, error)
	SaveBuyerInitialMessage(ctx context.Context, cmd SaveBuyerInitialMessageCommand) (*SaveBuyerInitialMessageResult, error)
	AttachFile(ctx context.Context, cmd AttachFileCommand) (*AttachFileResult, error)
}

// EventBroker abstracts the NATS JetStream broker implementation.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject, durable string, handler MessageHandler) error
	Close()
}

// MessageHandler processes one NATS JetStream message.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// Logger aliases the shared structured logger.
type Logger = logging.Logger

// Config carries application-level subject names owned by order-service.
type Config struct {
	OrderCreateResultSubject string
}

// OrderRepository aliases the domain write-model contract.
type OrderRepository = domain.OrderRepository

// OrderReadRepository aliases the domain read-model contract.
type OrderReadRepository = domain.OrderReadRepository

// CreateOrderCommand carries the immutable order snapshot from the saga.
type CreateOrderCommand struct {
	SagaID              string
	OrderID             string
	BuyerID             string
	SellerID            string
	GigID               string
	GigTitle            string
	PackageID           string
	PackageTier         string
	PackageDescription  string
	PackageDeliveryDays int32
	PriceCents          int64
	Currency            string
	IdempotencyKey      string
	RequestedAt         string
}

// ConfirmOrderCommand marks an order as paid after payment capture.
type ConfirmOrderCommand struct {
	SagaID          string
	OrderID         string
	PaymentIntentID string
	OccurredAt      string
}

// FailOrderCommand marks an order as failed when the saga cannot complete.
type FailOrderCommand struct {
	SagaID     string
	OrderID    string
	Reason     string
	OccurredAt string
}

// CreateDraftOrderCommand persists the authoritative order draft snapshot.
type CreateDraftOrderCommand struct {
	SagaID              string
	OrderID             string
	BuyerID             string
	SellerID            string
	GigID               string
	GigTitle            string
	PackageID           string
	PackageTier         string
	PackageDescription  string
	PackageDeliveryDays int32
	PriceCents          int64
	Currency            string
	Questions           []OrderQuestionSnapshot
	IdempotencyKey      string
	RequestedAt         string
}

// OrderQuestionSnapshot stores one immutable question snapshot for service commands.
type OrderQuestionSnapshot struct {
	QuestionID  string
	Text        string
	Type        string
	Required    bool
	OptionsJSON string
	SortOrder   int32
}

// CreateDraftOrderResult reports the created draft.
type CreateDraftOrderResult struct {
	OrderID string
	Status  string
}

// OrderPaymentSnapshot returns the current immutable payment-facing snapshot.
type OrderPaymentSnapshot struct {
	OrderID      string
	SagaID       string
	BuyerID      string
	SellerID     string
	GigTitle     string
	PackageTitle string
	PriceCents   int64
	Currency     string
	Status       string
}

// MarkPaymentPendingCommand records a checkout session against the order.
type MarkPaymentPendingCommand struct {
	OrderID         string
	PaymentIntentID string
	CheckoutURL     string
	OccurredAt      string
}

// MarkPaymentPendingResult reports the updated order state.
type MarkPaymentPendingResult struct {
	OrderID string
	Status  string
}

// MarkOrderFundedCommand marks an order funded after webhook success.
type MarkOrderFundedCommand struct {
	OrderID         string
	PaymentIntentID string
	OccurredAt      string
}

// MarkOrderFundedResult reports the updated order state.
type MarkOrderFundedResult struct {
	OrderID string
	Status  string
}

// MarkPaymentFailedCommand marks an order failed after webhook failure.
type MarkPaymentFailedCommand struct {
	OrderID string
	Reason  string
}

// MarkPaymentFailedResult reports the updated order state.
type MarkPaymentFailedResult struct {
	OrderID string
	Status  string
}

// SaveRequirementAnswersCommand stores buyer requirements answers.
type SaveRequirementAnswersCommand struct {
	OrderID string
	Answers []OrderAnswer
}

// OrderAnswer stores one buyer answer snapshot for service commands.
type OrderAnswer struct {
	QuestionID string
	Value      string
}

// SaveRequirementAnswersResult reports the updated order state.
type SaveRequirementAnswersResult struct {
	OrderID string
	Status  string
}

// SaveBuyerInitialMessageCommand stores buyer initial message.
type SaveBuyerInitialMessageCommand struct {
	OrderID string
	Message string
}

// SaveBuyerInitialMessageResult reports the updated order state.
type SaveBuyerInitialMessageResult struct {
	OrderID string
	Status  string
}

// AttachFileCommand stores one attachment reference.
type AttachFileCommand struct {
	OrderID      string
	AttachmentID string
}

// AttachFileResult reports the updated order state.
type AttachFileResult struct {
	OrderID string
	Status  string
}
