package model

import "time"

// OrderRow is the Yugabyte persistence model for orders.
type OrderRow struct {
	OrderID             string    `db:"order_id"`
	SagaID              string    `db:"saga_id"`
	BuyerID             string    `db:"buyer_id"`
	SellerID            string    `db:"seller_id"`
	GigID               string    `db:"gig_id"`
	GigTitle            string    `db:"gig_title"`
	PackageID           string    `db:"package_id"`
	PackageTier         string    `db:"package_tier"`
	PackageDescription  string    `db:"package_description"`
	PackageDeliveryDays int32     `db:"package_delivery_days"`
	PriceCents          int64     `db:"price_cents"`
	Currency            string    `db:"currency"`
	Status              string    `db:"status"`
	IdempotencyKey      string    `db:"idempotency_key"`
	PaymentIntentID     string    `db:"payment_intent_id"`
	FailureReason       string    `db:"failure_reason"`
	CreatedAt           time.Time `db:"created_at"`
	UpdatedAt           time.Time `db:"updated_at"`
}

// OrderGigSnapshotRow is the Yugabyte persistence model for the immutable gig snapshot.
type OrderGigSnapshotRow struct {
	OrderID             string    `db:"order_id"`
	GigID               string    `db:"gig_id"`
	GigTitle            string    `db:"gig_title"`
	PackageID           string    `db:"package_id"`
	PackageTier         string    `db:"package_tier"`
	PackageDescription  string    `db:"package_description"`
	PackageDeliveryDays int32     `db:"package_delivery_days"`
	PriceCents          int64     `db:"price_cents"`
	Currency            string    `db:"currency"`
	CreatedAt           time.Time `db:"created_at"`
}
