package redis

import (
	"testing"
	"time"

	"order-service/internal/domain"
)

func TestMapDeliveryToCache(t *testing.T) {
	cache := mapDeliveryToCache(&domain.OrderDeliveryProjection{
		OrderID: "order-1",
		Delivery: &domain.OrderDelivery{
			OrderID:         "order-1",
			SellerID:        "seller-1",
			DeliveryMessage: "done",
			CreatedAt:       time.Unix(100, 0).UTC(),
		},
		DeliveryFiles: []domain.OrderDeliveryFile{
			{
				OrderID:   "order-1",
				FileID:    "file-1",
				FileURL:   "https://cdn.example.com/file-1.png",
				SortOrder: 1,
				CreatedAt: time.Unix(200, 0).UTC(),
			},
		},
	})
	if cache.OrderDelivery.DeliveryMessage != "done" {
		t.Fatalf("cache = %#v", cache)
	}
	if got := len(cache.OrderDeliveryFiles); got != 1 {
		t.Fatalf("files = %d, want 1", got)
	}
	if cache.OrderDeliveryFiles[0].FileURL != "https://cdn.example.com/file-1.png" {
		t.Fatalf("file = %#v", cache.OrderDeliveryFiles[0])
	}
}
