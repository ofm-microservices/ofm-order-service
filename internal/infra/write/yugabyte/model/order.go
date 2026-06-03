package model

import "time"

// OrderRow is the Yugabyte persistence model for orders.
type OrderRow struct {
	OrderID               string    `db:"order_id"`
	SagaID                string    `db:"saga_id"`
	BuyerID               string    `db:"buyer_id"`
	SellerID              string    `db:"seller_id"`
	SellerUsername        string    `db:"seller_username"`
	GigID                 string    `db:"gig_id"`
	GigTitle              string    `db:"gig_title"`
	PackageID             string    `db:"package_id"`
	PackageTier           string    `db:"package_tier"`
	PackageDescription    string    `db:"package_description"`
	PackageDeliveryDays   int32     `db:"package_delivery_days"`
	PriceCents            int64     `db:"price_cents"`
	Currency              string    `db:"currency"`
	Status                string    `db:"status"`
	IdempotencyKey        string    `db:"idempotency_key"`
	PaymentIntentID       string    `db:"payment_intent_id"`
	PaymentReleaseID      string    `db:"payment_release_id"`
	FailureReason         string    `db:"failure_reason"`
	DeliveredAt           time.Time `db:"delivered_at"`
	CompletedAt           time.Time `db:"completed_at"`
	DisputedAt            time.Time `db:"disputed_at"`
	BuyerResponseDeadline time.Time `db:"buyer_response_deadline"`
	RevisionCountUsed     int32     `db:"revision_count_used"`
	CreatedAt             time.Time `db:"created_at"`
	UpdatedAt             time.Time `db:"updated_at"`
}

// OrderGigSnapshotRow is the Yugabyte persistence model for the immutable gig snapshot.
type OrderGigSnapshotRow struct {
	OrderID             string    `db:"order_id"`
	GigID               string    `db:"gig_id"`
	SellerUsername      string    `db:"seller_username"`
	GigTitle            string    `db:"gig_title"`
	PackageID           string    `db:"package_id"`
	PackageTier         string    `db:"package_tier"`
	PackageDescription  string    `db:"package_description"`
	PackageDeliveryDays int32     `db:"package_delivery_days"`
	PriceCents          int64     `db:"price_cents"`
	Currency            string    `db:"currency"`
	CreatedAt           time.Time `db:"created_at"`
}
