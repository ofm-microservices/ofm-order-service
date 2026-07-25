package yugabyte

import (
	"context"
	"database/sql"
	"strings"
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
order_id, saga_id, buyer_id, seller_id, seller_username, gig_id, package_id, status, idempotency_key,
COALESCE(payment_intent_id::text, ''), COALESCE(payment_release_id::text, ''), COALESCE(failure_reason, ''),
COALESCE(delivered_at, 'epoch'::timestamptz), COALESCE(completed_at, 'epoch'::timestamptz),
COALESCE(disputed_at, 'epoch'::timestamptz), COALESCE(buyer_response_deadline, 'epoch'::timestamptz),
COALESCE(revision_count_used, 0), created_at, updated_at`

const orderSnapshotColumns = `
gig_id, seller_username, gig_title, picture_file_id, package_id, package_tier, package_description, package_delivery_days,
price_cents, currency, created_at`

const createOrderQuery = `
INSERT INTO orders (
	order_id, saga_id, buyer_id, seller_id, seller_username, gig_id, package_id, status, idempotency_key,
	created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (order_id) DO UPDATE SET updated_at = orders.updated_at
RETURNING ` + orderColumns

const createOrderSnapshotQuery = `
INSERT INTO order_gig_snapshot (
	order_id, gig_id, seller_username, gig_title, picture_file_id, package_id, package_tier, package_description, package_delivery_days,
	price_cents, currency, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
ON CONFLICT (order_id) DO UPDATE SET
	gig_id = EXCLUDED.gig_id,
	seller_username = EXCLUDED.seller_username,
	gig_title = EXCLUDED.gig_title,
	picture_file_id = EXCLUDED.picture_file_id,
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

const markOrderFundedQuery = `
UPDATE orders
SET status = $2, payment_intent_id = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const markOrderPaymentSessionQuery = `
INSERT INTO order_checkout_sessions (order_id, checkout_url, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
ON CONFLICT (order_id) DO UPDATE SET checkout_url = EXCLUDED.checkout_url, updated_at = NOW()
`

const markOrderCheckoutPendingQuery = `
UPDATE orders
SET status = $2, payment_intent_id = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const markOrderFailedQuery = `
UPDATE orders
SET status = $2, failure_reason = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const saveDeliveryQuery = `
UPDATE orders
SET status = $2, delivered_at = NOW(), updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const getOrderDeliveryQuery = `
SELECT order_id, seller_id, delivery_message, created_at
FROM order_deliveries
WHERE order_id = $1
`

const getOrderDeliveryFilesQuery = `
SELECT order_id, file_id, sort_order, created_at
FROM order_delivery_files
WHERE order_id = $1
ORDER BY sort_order ASC, file_id ASC
`

const getOrderCountByGigIDQuery = `
SELECT COUNT(*)
FROM orders
WHERE gig_id = $1
  AND status NOT IN ('draft', 'failed')
`

const upsertOrderDeliveryQuery = `
INSERT INTO order_deliveries (order_id, seller_id, delivery_message, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (order_id) DO UPDATE SET
    seller_id = EXCLUDED.seller_id,
    delivery_message = EXCLUDED.delivery_message
`

const insertDeliveryFileQuery = `
INSERT INTO order_delivery_files (order_id, file_id, sort_order, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (order_id, file_id) DO UPDATE SET sort_order = EXCLUDED.sort_order
`

const markReleasePendingQuery = `
UPDATE orders
SET status = $2, payment_release_id = NULLIF($3, '')::uuid, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const requestRevisionQuery = `
UPDATE orders
SET status = $2, revision_count_used = revision_count_used + 1, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const openDisputeQuery = `
UPDATE orders
SET status = $2, disputed_at = NOW(), updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const upsertOrderDisputeQuery = `
INSERT INTO order_disputes (order_id, buyer_id, initiator_user_id, initiator_role, dispute_type, reason, created_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (order_id) DO UPDATE SET
	buyer_id = EXCLUDED.buyer_id,
	initiator_user_id = EXCLUDED.initiator_user_id,
	initiator_role = EXCLUDED.initiator_role,
	dispute_type = EXCLUDED.dispute_type,
	reason = EXCLUDED.reason
`

const markOrderCompletedQuery = `
UPDATE orders
SET status = $2, payment_release_id = NULLIF($3, '')::uuid, completed_at = NOW(), updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const markDisputeResolvedQuery = `
UPDATE orders
SET status = $2, payment_release_id = NULLIF($3, '')::uuid, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const markReleaseFailedQuery = `
UPDATE orders
SET status = $2, failure_reason = $3, updated_at = NOW()
WHERE order_id = $1
RETURNING ` + orderColumns

const saveRequirementAnswersQuery = `
INSERT INTO order_requirement_answers (order_id, question_id, answer_value, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (order_id, question_id) DO UPDATE SET answer_value = EXCLUDED.answer_value
`

const hasConfirmPrerequisitesQuery = `
SELECT
	NOT EXISTS (
		SELECT 1
		FROM order_question_snapshots q
		WHERE q.order_id = $1
		  AND q.required = TRUE
		  AND NOT EXISTS (
			SELECT 1
			FROM order_requirement_answers a
			WHERE a.order_id = q.order_id
			  AND a.question_id = q.question_id
		  )
	) AS requirements_completed,
	EXISTS (
		SELECT 1
		FROM order_buyer_messages m
		WHERE m.order_id = $1
		  AND NULLIF(trim(m.message), '') IS NOT NULL
	) AS message_completed
`

const saveBuyerInitialMessageQuery = `
INSERT INTO order_buyer_messages (order_id, message, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
ON CONFLICT (order_id) DO UPDATE SET message = EXCLUDED.message, updated_at = NOW()
`

const hasBuyerInitialMessageQuery = `
SELECT EXISTS (
	SELECT 1
	FROM order_buyer_messages
	WHERE order_id = $1
	  AND NULLIF(trim(message), '') IS NOT NULL
)
`

const getRequirementsSnapshotByIDQuery = `
SELECT
	o.order_id,
	o.buyer_id,
	o.seller_id,
	q.question_id,
	q.text,
	q.type,
	q.required,
	q.sort_order,
	a.answer_value,
	m.message,
	m.created_at,
	m.updated_at
FROM orders o
LEFT JOIN order_question_snapshots q ON q.order_id = o.order_id
LEFT JOIN order_requirement_answers a ON a.order_id = q.order_id AND a.question_id = q.question_id
LEFT JOIN order_buyer_messages m ON m.order_id = o.order_id
WHERE o.order_id = $1
ORDER BY q.sort_order ASC, q.question_id ASC
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
		params.SellerUsername,
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
		params.SellerUsername,
		params.GigTitle,
		params.PictureFileID,
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

func (r *repo) MarkFunded(ctx context.Context, orderID, paymentIntentID string) (*domain.Order, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "mark_funded", "orders", status, time.Since(started)) }()

	if _, err := scanOrder(r.db.QueryRowContext(ctx, markOrderFundedQuery, orderID, domain.OrderStatusFunded, paymentIntentID)); err != nil {
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

func (r *repo) HasConfirmPrerequisites(ctx context.Context, orderID string) (bool, bool, error) {
	var requirementsCompleted bool
	var messageCompleted bool
	if err := r.db.QueryRowContext(ctx, hasConfirmPrerequisitesQuery, orderID).Scan(&requirementsCompleted, &messageCompleted); err != nil {
		return false, false, err
	}
	return requirementsCompleted, messageCompleted, nil
}

func (r *repo) SaveBuyerInitialMessage(ctx context.Context, params domain.SaveBuyerInitialMessageParams) (*domain.Order, error) {
	if _, err := r.db.ExecContext(ctx, saveBuyerInitialMessageQuery, params.OrderID, params.Message); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) GetRequirementsByID(ctx context.Context, orderID string) (*domain.OrderRequirements, error) {
	rows, err := r.db.QueryContext(ctx, getRequirementsSnapshotByIDQuery, orderID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	defer rows.Close()

	out := &domain.OrderRequirements{
		QuestionsAnswers: make([]domain.OrderRequirementQuestionAnswer, 0),
	}
	hasQuestion := false
	seenMessage := false
	for rows.Next() {
		var (
			rowOrderID  string
			buyerID     string
			sellerID    string
			questionID  sql.NullString
			text        sql.NullString
			qType       sql.NullString
			required    sql.NullBool
			sortOrder   sql.NullInt32
			answerValue sql.NullString
			message     sql.NullString
			createdAt   sql.NullTime
			updatedAt   sql.NullTime
		)
		if err := rows.Scan(&rowOrderID, &buyerID, &sellerID, &questionID, &text, &qType, &required, &sortOrder, &answerValue, &message, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		out.OrderID = rowOrderID
		out.BuyerID = buyerID
		out.SellerID = sellerID
		if questionID.Valid {
			hasQuestion = true
			qa := domain.OrderRequirementQuestionAnswer{
				Question: &domain.OrderRequirementQuestion{
					QuestionID: questionID.String,
					Text:       text.String,
					Type:       qType.String,
					Required:   required.Bool,
					SortOrder:  sortOrder.Int32,
				},
			}
			if answerValue.Valid {
				qa.Answer = &domain.OrderRequirementAnswer{Value: answerValue.String}
			}
			out.QuestionsAnswers = append(out.QuestionsAnswers, qa)
		}
		if !seenMessage && message.Valid && strings.TrimSpace(message.String) != "" {
			out.CustomerMessage = &domain.OrderRequirementCustomerMessage{
				Message:   message.String,
				CreatedAt: createdAt.Time,
				UpdatedAt: updatedAt.Time,
			}
			seenMessage = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.OrderID) == "" {
		return nil, domain.ErrOrderNotFound
	}
	if !hasQuestion && !seenMessage {
		return nil, domain.ErrOrderNotFound
	}
	return out, nil
}

func (r *repo) AttachFile(ctx context.Context, params domain.AttachFileParams) (*domain.Order, error) {
	if _, err := r.db.ExecContext(ctx, attachFileQuery, params.OrderID, params.AttachmentID, params.FileID, params.SortOrder); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) SaveCheckoutSession(ctx context.Context, orderID, paymentIntentID, checkoutURL string) error {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, markOrderCheckoutPendingQuery, orderID, domain.OrderStatusPaymentPending, paymentIntentID)); err != nil {
		if err == sql.ErrNoRows {
			return domain.ErrOrderNotFound
		}
		return err
	}
	_, err := r.db.ExecContext(ctx, markOrderPaymentSessionQuery, orderID, checkoutURL)
	return err
}

func (r *repo) GetLifecycleSnapshot(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.GetByID(ctx, orderID)
}

func (r *repo) SaveDelivery(ctx context.Context, params domain.SaveDeliveryParams) (*domain.Order, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := scanOrder(tx.QueryRowContext(ctx, saveDeliveryQuery, params.OrderID, domain.OrderStatusDelivered)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, upsertOrderDeliveryQuery, params.OrderID, params.SellerID, params.Message); err != nil {
		return nil, err
	}
	for i, attachmentID := range params.AttachmentIDs {
		if strings.TrimSpace(attachmentID) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, insertDeliveryFileQuery, params.OrderID, attachmentID, i+1); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) GetDeliveryByID(ctx context.Context, orderID string) (*domain.OrderDeliveryProjection, error) {
	var delivery model.OrderDeliveryRow
	if err := r.db.QueryRowContext(ctx, getOrderDeliveryQuery, orderID).Scan(
		&delivery.OrderID,
		&delivery.SellerID,
		&delivery.DeliveryMessage,
		&delivery.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, getOrderDeliveryFilesQuery, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &domain.OrderDeliveryProjection{
		OrderID: orderID,
		Delivery: &domain.OrderDelivery{
			OrderID:         delivery.OrderID,
			SellerID:        delivery.SellerID,
			DeliveryMessage: delivery.DeliveryMessage,
			CreatedAt:       delivery.CreatedAt,
		},
		DeliveryFiles: make([]domain.OrderDeliveryFile, 0),
	}
	for rows.Next() {
		var file model.OrderDeliveryFileRow
		if err := rows.Scan(&file.OrderID, &file.FileID, &file.SortOrder, &file.CreatedAt); err != nil {
			return nil, err
		}
		out.DeliveryFiles = append(out.DeliveryFiles, domain.OrderDeliveryFile{
			OrderID:   file.OrderID,
			FileID:    file.FileID,
			SortOrder: file.SortOrder,
			CreatedAt: file.CreatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *repo) GetOrderCountByGigID(ctx context.Context, gigID string) (int64, error) {
	var count int64
	if err := r.db.QueryRowContext(ctx, getOrderCountByGigIDQuery, gigID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *repo) MarkReleasePending(ctx context.Context, orderID, paymentReleaseID string) (*domain.Order, error) {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, markReleasePendingQuery, orderID, domain.OrderStatusReleasePending, paymentReleaseID)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) RequestRevision(ctx context.Context, params domain.RequestRevisionParams) (*domain.Order, error) {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, requestRevisionQuery, params.OrderID, domain.OrderStatusRevisionRequested)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) OpenDispute(ctx context.Context, params domain.OpenDisputeParams) (*domain.Order, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	order, err := scanOrder(tx.QueryRowContext(ctx, openDisputeQuery, params.OrderID, domain.OrderStatusDisputed))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	initiatorRole := "seller"
	if strings.TrimSpace(params.InitiatorID) == strings.TrimSpace(order.BuyerID) {
		initiatorRole = "buyer"
	}
	if _, err := tx.ExecContext(ctx, upsertOrderDisputeQuery, params.OrderID, order.BuyerID, params.InitiatorID, initiatorRole, params.DisputeType, params.Reason); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, params.OrderID)
}

func (r *repo) MarkCompleted(ctx context.Context, orderID, paymentReleaseID string) (*domain.Order, error) {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, markOrderCompletedQuery, orderID, domain.OrderStatusCompleted, paymentReleaseID)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) MarkDisputeResolved(ctx context.Context, orderID, paymentReleaseID string) (*domain.Order, error) {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, markDisputeResolvedQuery, orderID, domain.OrderStatusDisputeResolved, paymentReleaseID)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) MarkReleaseFailed(ctx context.Context, orderID, reason string) (*domain.Order, error) {
	if _, err := scanOrder(r.db.QueryRowContext(ctx, markReleaseFailedQuery, orderID, domain.OrderStatusReleaseFailed, reason)); err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetByID(ctx, orderID)
}

func (r *repo) scanOrderWithSnapshot(ctx context.Context, orderID string) (*domain.Order, error) {
	var order model.OrderRow
	if err := r.db.QueryRowContext(ctx, getOrderByIDQuery, orderID).Scan(
		&order.OrderID,
		&order.SagaID,
		&order.BuyerID,
		&order.SellerID,
		&order.SellerUsername,
		&order.GigID,
		&order.PackageID,
		&order.Status,
		&order.IdempotencyKey,
		&order.PaymentIntentID,
		&order.PaymentReleaseID,
		&order.FailureReason,
		&order.DeliveredAt,
		&order.CompletedAt,
		&order.DisputedAt,
		&order.BuyerResponseDeadline,
		&order.RevisionCountUsed,
		&order.CreatedAt,
		&order.UpdatedAt,
	); err != nil {
		return nil, err
	}
	var snap model.OrderGigSnapshotRow
	if err := r.db.QueryRowContext(ctx, getOrderSnapshotByIDQuery, orderID).Scan(
		&snap.OrderID,
		&snap.GigID,
		&snap.SellerUsername,
		&snap.GigTitle,
		&snap.PictureFileID,
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
	order.SellerUsername = snap.SellerUsername
	order.GigTitle = snap.GigTitle
	order.PictureFileID = snap.PictureFileID
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
		&row.SellerUsername,
		&row.GigID,
		&row.PackageID,
		&row.Status,
		&row.IdempotencyKey,
		&row.PaymentIntentID,
		&row.PaymentReleaseID,
		&row.FailureReason,
		&row.DeliveredAt,
		&row.CompletedAt,
		&row.DisputedAt,
		&row.BuyerResponseDeadline,
		&row.RevisionCountUsed,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	return row, err
}

func mapRow(row model.OrderRow) *domain.Order {
	return &domain.Order{
		OrderID:               row.OrderID,
		SagaID:                row.SagaID,
		BuyerID:               row.BuyerID,
		SellerID:              row.SellerID,
		SellerUsername:        row.SellerUsername,
		GigID:                 row.GigID,
		GigTitle:              row.GigTitle,
		PictureFileID:         row.PictureFileID,
		PackageID:             row.PackageID,
		PackageTier:           row.PackageTier,
		PackageDescription:    row.PackageDescription,
		PackageDeliveryDays:   row.PackageDeliveryDays,
		PriceCents:            row.PriceCents,
		Currency:              row.Currency,
		Status:                row.Status,
		IdempotencyKey:        row.IdempotencyKey,
		PaymentIntentID:       row.PaymentIntentID,
		PaymentReleaseID:      row.PaymentReleaseID,
		FailureReason:         row.FailureReason,
		DeliveredAt:           row.DeliveredAt,
		CompletedAt:           row.CompletedAt,
		DisputedAt:            row.DisputedAt,
		BuyerResponseDeadline: row.BuyerResponseDeadline,
		RevisionCountUsed:     row.RevisionCountUsed,
		CreatedAt:             row.CreatedAt,
		UpdatedAt:             row.UpdatedAt,
	}
}
