package domain

import (
	"context"
	"time"
)

const (
	OrderStatusDraft                 = "draft"
	OrderStatusRequirementsPending   = "requirements_pending"
	OrderStatusRequirementsCompleted = "requirements_completed"
	OrderStatusMessageCompleted      = "message_completed"
	OrderStatusAttachmentsPending    = "attachments_pending"
	OrderStatusReadyToPay            = "ready_to_pay"
	OrderStatusPaymentPending        = "payment_pending"
	OrderStatusFunded                = "funded"
	OrderStatusPaid                  = "paid"
	OrderStatusDelivered             = "delivered"
	OrderStatusReleasePending        = "release_pending"
	OrderStatusRevisionRequested     = "revision_requested"
	OrderStatusDisputed              = "disputed"
	OrderStatusCompleted             = "completed"
	OrderStatusReleaseFailed         = "release_failed"
	OrderStatusFailed                = "failed"
)

// Order is the immutable order snapshot plus lifecycle state owned by
// order-service.
type Order struct {
	OrderID               string
	SagaID                string
	BuyerID               string
	SellerID              string
	SellerUsername        string
	GigID                 string
	GigTitle              string
	PackageID             string
	PackageTier           string
	PackageDescription    string
	PackageDeliveryDays   int32
	PriceCents            int64
	Currency              string
	Status                string
	IdempotencyKey        string
	PaymentIntentID       string
	PaymentReleaseID      string
	FailureReason         string
	DeliveredAt           time.Time
	CompletedAt           time.Time
	DisputedAt            time.Time
	BuyerResponseDeadline time.Time
	RevisionCountUsed     int32
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// CreateOrderParams carries the snapshot used to create an order.
type CreateOrderParams struct {
	OrderID             string
	SagaID              string
	BuyerID             string
	SellerID            string
	SellerUsername      string
	GigID               string
	GigTitle            string
	PackageID           string
	PackageTier         string
	PackageDescription  string
	PackageDeliveryDays int32
	PriceCents          int64
	Currency            string
	IdempotencyKey      string
	RequestedAt         time.Time
}

// OrderAnswer stores one buyer answer snapshot.
type OrderAnswer struct {
	QuestionID string
	Value      string
}

// OrderQuestionSnapshot stores one immutable question snapshot on the order.
type OrderQuestionSnapshot struct {
	OrderID     string
	QuestionID  string
	Text        string
	Type        string
	Required    bool
	OptionsJSON string
	SortOrder   int32
}

// SaveQuestionSnapshotsParams stores the immutable question snapshot set.
type SaveQuestionSnapshotsParams struct {
	OrderID   string
	Questions []OrderQuestionSnapshot
}

// SaveRequirementAnswersParams stores the buyer answers snapshot.
type SaveRequirementAnswersParams struct {
	OrderID string
	Answer  OrderAnswer
}

// SaveBuyerInitialMessageParams stores the buyer initial message snapshot.
type SaveBuyerInitialMessageParams struct {
	OrderID string
	Message string
}

// AttachFileParams stores one attachment reference on the order.
type AttachFileParams struct {
	OrderID      string
	AttachmentID string
	FileID       string
	SortOrder    int32
}

// OrderRepository persists the order write model.
type OrderRepository interface {
	Create(ctx context.Context, params CreateOrderParams) (*Order, error)
	SaveQuestionSnapshots(ctx context.Context, params SaveQuestionSnapshotsParams) error
	GetByID(ctx context.Context, orderID string) (*Order, error)
	MarkPaid(ctx context.Context, orderID, paymentIntentID string) (*Order, error)
	MarkFunded(ctx context.Context, orderID, paymentIntentID string) (*Order, error)
	MarkFailed(ctx context.Context, orderID, reason string) (*Order, error)
	SaveRequirementAnswers(ctx context.Context, params SaveRequirementAnswersParams) (*Order, error)
	SaveBuyerInitialMessage(ctx context.Context, params SaveBuyerInitialMessageParams) (*Order, error)
	AttachFile(ctx context.Context, params AttachFileParams) (*Order, error)
	SaveCheckoutSession(ctx context.Context, orderID, paymentIntentID, checkoutURL string) error
	GetLifecycleSnapshot(ctx context.Context, orderID string) (*Order, error)
	SaveDelivery(ctx context.Context, params SaveDeliveryParams) (*Order, error)
	MarkReleasePending(ctx context.Context, orderID, paymentReleaseID string) (*Order, error)
	RequestRevision(ctx context.Context, params RequestRevisionParams) (*Order, error)
	OpenDispute(ctx context.Context, params OpenDisputeParams) (*Order, error)
	MarkCompleted(ctx context.Context, orderID, paymentReleaseID string) (*Order, error)
	MarkReleaseFailed(ctx context.Context, orderID, reason string) (*Order, error)
}

// OrderGigSnapshot stores the immutable gig snapshot persisted with an order.
type OrderGigSnapshot struct {
	OrderID             string
	GigID               string
	GigTitle            string
	PackageID           string
	PackageTier         string
	PackageDescription  string
	PackageDeliveryDays int32
	PriceCents          int64
	Currency            string
	SellerUsername      string
}

// SaveDeliveryParams stores one seller delivery transition.
type SaveDeliveryParams struct {
	OrderID       string
	SellerID      string
	Message       string
	AttachmentIDs []string
	RequestedAt   time.Time
}

// RequestRevisionParams stores one buyer revision request.
type RequestRevisionParams struct {
	OrderID     string
	BuyerID     string
	Reason      string
	RequestedAt time.Time
}

// OpenDisputeParams stores one buyer dispute transition.
type OpenDisputeParams struct {
	OrderID     string
	BuyerID     string
	Reason      string
	RequestedAt time.Time
}

// OrderReadRepository persists the order read-model projection.
type OrderReadRepository interface {
	Upsert(ctx context.Context, order *Order) error
}
