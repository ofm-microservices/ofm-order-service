package model

// DeliveryCache is the Redis projection model for a seller delivery page.
type DeliveryCache struct {
	OrderDelivery      OrderDeliverySnapshot  `json:"order_delivery"`
	OrderDeliveryFiles []DeliveryFileSnapshot `json:"order_delivery_files"`
}

// OrderDeliverySnapshot stores the delivery header in the Redis projection.
type OrderDeliverySnapshot struct {
	DeliveryMessage string `json:"delivery_message"`
	CreatedAt       string `json:"created_at"`
}

// DeliveryFileSnapshot stores one delivery file in the Redis projection.
type DeliveryFileSnapshot struct {
	FileID    string `json:"file_id"`
	FileURL   string `json:"file_url"`
	SortOrder int32  `json:"sort_order"`
	CreatedAt string `json:"created_at"`
}
