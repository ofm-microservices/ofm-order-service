package model

// OrderCache is the Redis projection model for an order.
type OrderCache struct {
	OrderID             string `json:"order_id"`
	SagaID              string `json:"saga_id"`
	BuyerID             string `json:"buyer_id"`
	SellerID            string `json:"seller_id"`
	GigID               string `json:"gig_id"`
	GigTitle            string `json:"gig_title"`
	PackageID           string `json:"package_id"`
	PackageTier         string `json:"package_tier"`
	PackageDescription  string `json:"package_description"`
	PackageDeliveryDays int32  `json:"package_delivery_days"`
	PriceCents          int64  `json:"price_cents"`
	Currency            string `json:"currency"`
	Status              string `json:"status"`
	PaymentIntentID     string `json:"payment_intent_id,omitempty"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
}
