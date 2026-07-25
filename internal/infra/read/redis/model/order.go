package model

// OrderCache is the Redis projection model for an order page snapshot.
type OrderCache struct {
	Order           OrderSnapshot `json:"order"`
	Gig             GigSnapshot   `json:"gig"`
	Customer        *UserSnapshot `json:"customer,omitempty"`
	Freelancer      *UserSnapshot `json:"freelancer,omitempty"`
	BuyerID         string        `json:"buyer_id,omitempty"`
	SellerID        string        `json:"seller_id,omitempty"`
	SellerUsername  string        `json:"seller_username,omitempty"`
	PictureFileID   string        `json:"picture_file_id,omitempty"`
	PaymentIntentID string        `json:"payment_intent_id,omitempty"`
}

// OrderSnapshot stores the order header in the Redis page projection.
type OrderSnapshot struct {
	OrderID   string `json:"order_id"`
	CreatedAt string `json:"created_at"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// GigSnapshot stores the gig block in the Redis page projection.
type GigSnapshot struct {
	GigID      string          `json:"gig_id"`
	Title      string          `json:"title"`
	PictureURL string          `json:"picture_url"`
	Package    PackageSnapshot `json:"package"`
}

// PackageSnapshot stores the nested package block in the Redis page projection.
type PackageSnapshot struct {
	PackageID           string `json:"package_id"`
	Title               string `json:"title"`
	PriceCents          int64  `json:"price_cents"`
	Currency            string `json:"currency"`
	Description         string `json:"description"`
	PackageDeliveryDays int32  `json:"package_delivery_days"`
}

// UserSnapshot stores the participant snapshot in the Redis page projection.
type UserSnapshot struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
}

// LegacyOrderCache keeps compatibility with the previous flat Redis payload.
type LegacyOrderCache struct {
	OrderID             string `json:"order_id"`
	SagaID              string `json:"saga_id"`
	BuyerID             string `json:"buyer_id"`
	SellerID            string `json:"seller_id"`
	SellerUsername      string `json:"seller_username,omitempty"`
	GigID               string `json:"gig_id"`
	GigTitle            string `json:"gig_title"`
	PictureFileID       string `json:"picture_file_id,omitempty"`
	PictureURL          string `json:"picture_url,omitempty"`
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
