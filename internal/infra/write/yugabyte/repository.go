package yugabyte

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"order-service/internal/domain"
	"order-service/internal/infra/write/yugabyte/model"
)

type repo struct {
	db  *sqlx.DB
	log logging.Logger
}

// New constructs the Yugabyte-backed order repository.
func New(db *sqlx.DB, log logging.Logger) (domain.OrderRepository, error) {
	if db == nil {
		return nil, ErrNilYugaByteDB
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &repo{db: db, log: log.With(logging.String("module", "yugabyte-repository"))}, nil
}

const orderColumns = `
order_id, saga_id, buyer_id, seller_id, gig_id, package_id, status, idempotency_key,
COALESCE(payment_intent_id::text, ''), COALESCE(failure_reason, ''), created_at, updated_at`

const orderSnapshotColumns = `
gig_id, gig_title, package_id, package_tier, package_description, package_delivery_days,
price_cents, currency, created_at`

const createOrderQuery = `
INSERT INTO orders (
	order_id, saga_id, buyer_id, seller_id, gig_id, package_id, status, idempotency_key,
	created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ON CONFLICT (order_id) DO UPDATE SET updated_at = orders.updated_at
RETURNING ` + orderColumns

const createOrderSnapshotQuery = `
INSERT INTO order_gig_snapshot (
	order_id, gig_id, gig_title, package_id, package_tier, package_description, package_delivery_days,
	price_cents, currency, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (order_id) DO UPDATE SET
	gig_id = EXCLUDED.gig_id,
	gig_title = EXCLUDED.gig_title,
	package_id = EXCLUDED.package_id,
	package_tier = EXCLUDED.package_tier,
	package_description = EXCLUDED.package_description,
	package_delivery_days = EXCLUDED.package_delivery_days,
	price_cents = EXCLUDED.price_cents,
	currency = EXCLUDED.currency
RETURNING order_id, ` + orderSnapshotColumns

const getOrderSnapshotByIDQuery = `SELECT order_id, ` + orderSnapshotColumns + ` FROM order_gig_snapshot WHERE order_id = $1`

const getOrderByIDQuery = `SELECT ` + orderColumns + ` FROM orders WHERE order_id = $1`

const insertQuestionSnapshotQuery = `
INSERT INTO order_question_snapshots (
	order_id, question_id, text, type, required, options_json, sort_order, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (order_id, question_id) DO UPDATE SET
	text = EXCLUDED.text,
	type = EXCLUDED.type,
	required = EXCLUDED.required,
	options_json = EXCLUDED.options_json,
	sort_order = EXCLUDED.sort_order
`

const markOrderPaidQuery = `
UPDATE orders
SET status = $2, payment_intent_id = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const markOrderPaymentSessionQuery = `
INSERT INTO order_checkout_sessions (order_id, checkout_url, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
ON CONFLICT (order_id) DO UPDATE SET checkout_url = EXCLUDED.checkout_url, updated_at = NOW()
`

const markOrderFailedQuery = `
UPDATE orders
SET status = $2, failure_reason = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const saveRequirementAnswersQuery = `
INSERT INTO order_requirement_answers (order_id, question_id, answer_value, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (order_id, question_id) DO UPDATE SET answer_value = EXCLUDED.answer_value
`

const saveBuyerInitialMessageQuery = `
INSERT INTO order_buyer_messages (order_id, message, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
ON CONFLICT (order_id) DO UPDATE SET message = EXCLUDED.message, updated_at = NOW()
`

const attachFileQuery = `
INSERT INTO order_attachments (order_id, attachment_id, file_id, sort_order, created_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (order_id, attachment_id) DO UPDATE SET file_id = EXCLUDED.file_id, sort_order = EXCLUDED.sort_order
`

func (r *repo) Create(ctx context.Context, params domain.CreateOrderParams) (*domain.Order, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "create", "orders", status, time.Since(started)) }()

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := scanOrder(tx.QueryRowContext(ctx, createOrderQuery,
		params.OrderID,
		params.SagaID,
		params.BuyerID,
		params.SellerID,
		params.GigID,
		params.PackageID,
		domain.OrderStatusRequirementsPending,
		params.IdempotencyKey,
		params.RequestedAt,
		params.RequestedAt,
	)); err != nil {
		status = "error"
		r.log.Error("create order failed", logging.Operation("db.order.create"), logging.DurationMS(time.Since(started)), logging.String("order_id", params.OrderID), logging.Err(err))
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, createOrderSnapshotQuery,
		params.OrderID,
		params.GigID,
		params.GigTitle,
		params.PackageID,
		params.PackageTier,
		params.PackageDescription,
		params.PackageDeliveryDays,
		params.PriceCents,
		params.Currency,
	); err != nil {
		status = "error"
		r.log.Error("create order snapshot failed", logging.Operation("db.order.create_snapshot"), logging.DurationMS(time.Since(started)), logging.String("order_id", params.OrderID), logging.Err(err))
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		status = "error"
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) SaveQuestionSnapshots(ctx context.Context, params domain.SaveQuestionSnapshotsParams) error {
	for _, q := range params.Questions {
		if _, err := r.db.ExecContext(ctx, insertQuestionSnapshotQuery, params.OrderID, q.QuestionID, q.Text, q.Type, q.Required, q.OptionsJSON, q.SortOrder); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo) GetByID(ctx context.Context, orderID string) (*domain.Order, error) {
	row, err := r.scanOrderWithSnapshot(ctx, orderID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return row, nil
}

func (r *repo) MarkPaid(ctx context.Context, orderID, paymentIntentID string) (*domain.Order, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "mark_paid", "orders", status, time.Since(started)) }()

	if _, err := scanOrder(r.db.QueryRowContext(ctx, markOrderPaidQuery, orderID, domain.OrderStatusPaid, paymentIntentID)); err != nil {
		status = "error"
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) MarkFailed(ctx context.Context, orderID, reason string) (*domain.Order, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "mark_failed", "orders", status, time.Since(started)) }()

	if _, err := scanOrder(r.db.QueryRowContext(ctx, markOrderFailedQuery, orderID, domain.OrderStatusFailed, reason)); err != nil {
		status = "error"
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) SaveRequirementAnswers(ctx context.Context, params domain.SaveRequirementAnswersParams) (*domain.Order, error) {
	if _, err := r.db.ExecContext(ctx, saveRequirementAnswersQuery, params.OrderID, params.Answer.QuestionID, params.Answer.Value); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) SaveBuyerInitialMessage(ctx context.Context, params domain.SaveBuyerInitialMessageParams) (*domain.Order, error) {
	if _, err := r.db.ExecContext(ctx, saveBuyerInitialMessageQuery, params.OrderID, params.Message); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) AttachFile(ctx context.Context, params domain.AttachFileParams) (*domain.Order, error) {
	if _, err := r.db.ExecContext(ctx, attachFileQuery, params.OrderID, params.AttachmentID, params.FileID, params.SortOrder); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) SaveCheckoutSession(ctx context.Context, orderID, checkoutURL string) error {
	_, err := r.db.ExecContext(ctx, markOrderPaymentSessionQuery, orderID, checkoutURL)
	return err
}

func (r *repo) scanOrderWithSnapshot(ctx context.Context, orderID string) (*domain.Order, error) {
	var order model.OrderRow
	if err := r.db.QueryRowContext(ctx, getOrderByIDQuery, orderID).Scan(
		&order.OrderID,
		&order.SagaID,
		&order.BuyerID,
		&order.SellerID,
		&order.GigID,
		&order.PackageID,
		&order.Status,
		&order.IdempotencyKey,
		&order.PaymentIntentID,
		&order.FailureReason,
		&order.CreatedAt,
		&order.UpdatedAt,
	); err != nil {
		return nil, err
	}
	var snap model.OrderGigSnapshotRow
	if err := r.db.QueryRowContext(ctx, getOrderSnapshotByIDQuery, orderID).Scan(
		&snap.OrderID,
		&snap.GigID,
		&snap.GigTitle,
		&snap.PackageID,
		&snap.PackageTier,
		&snap.PackageDescription,
		&snap.PackageDeliveryDays,
		&snap.PriceCents,
		&snap.Currency,
		&snap.CreatedAt,
	); err != nil {
		return nil, err
	}
	order.GigID = snap.GigID
	order.GigTitle = snap.GigTitle
	order.PackageID = snap.PackageID
	order.PackageTier = snap.PackageTier
	order.PackageDescription = snap.PackageDescription
	order.PackageDeliveryDays = snap.PackageDeliveryDays
	order.PriceCents = snap.PriceCents
	order.Currency = snap.Currency
	return mapRow(order), nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(scanner rowScanner) (model.OrderRow, error) {
	var row model.OrderRow
	err := scanner.Scan(
		&row.OrderID,
		&row.SagaID,
		&row.BuyerID,
		&row.SellerID,
		&row.GigID,
		&row.PackageID,
		&row.Status,
		&row.IdempotencyKey,
		&row.PaymentIntentID,
		&row.FailureReason,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	return row, err
}

func mapRow(row model.OrderRow) *domain.Order {
	return &domain.Order{
		OrderID:             row.OrderID,
		SagaID:              row.SagaID,
		BuyerID:             row.BuyerID,
		SellerID:            row.SellerID,
		GigID:               row.GigID,
		GigTitle:            row.GigTitle,
		PackageID:           row.PackageID,
		PackageTier:         row.PackageTier,
		PackageDescription:  row.PackageDescription,
		PackageDeliveryDays: row.PackageDeliveryDays,
		PriceCents:          row.PriceCents,
		Currency:            row.Currency,
		Status:              row.Status,
		IdempotencyKey:      row.IdempotencyKey,
		PaymentIntentID:     row.PaymentIntentID,
		FailureReason:       row.FailureReason,
		CreatedAt:           row.CreatedAt,
		UpdatedAt:           row.UpdatedAt,
	}
}
