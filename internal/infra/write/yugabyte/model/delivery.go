package model

import "time"

// OrderDeliveryRow is the Yugabyte persistence model for a seller delivery.
type OrderDeliveryRow struct {
	OrderID         string    `db:"order_id"`
	SellerID        string    `db:"seller_id"`
	DeliveryMessage string    `db:"delivery_message"`
	CreatedAt       time.Time `db:"created_at"`
}

// OrderDeliveryFileRow is the Yugabyte persistence model for a delivery file.
type OrderDeliveryFileRow struct {
	OrderID   string    `db:"order_id"`
	FileID    string    `db:"file_id"`
	SortOrder int32     `db:"sort_order"`
	CreatedAt time.Time `db:"created_at"`
}
