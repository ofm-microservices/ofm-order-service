package redis

import (
	"testing"
	"time"

	"order-service/internal/domain"
	"order-service/internal/infra/read/redis/model"
)

func TestMapDomainToCacheAndBack(t *testing.T) {
	order := &domain.Order{
		OrderID:             "order-1",
		SagaID:              "saga-1",
		BuyerID:             "buyer-1",
		SellerID:            "seller-1",
		SellerUsername:      "seller-name",
		GigID:               "gig-1",
		GigTitle:            "Gig",
		PictureFileID:       "file-1",
		PictureURL:          "https://cdn.example.com/file-1.png",
		PackageID:           "pkg-1",
		PackageTier:         "basic",
		PackageDescription:  "Basic package",
		PackageDeliveryDays: 3,
		PriceCents:          1000,
		Currency:            "usd",
		Status:              domain.OrderStatusCompleted,
		PaymentIntentID:     "pi-1",
		CreatedAt:           time.Unix(100, 0).UTC(),
		UpdatedAt:           time.Unix(200, 0).UTC(),
		Customer: &domain.OrderPreviewUser{
			UserID:      "buyer-1",
			Username:    "alex1",
			DisplayName: "Buyer",
			AvatarURL:   "buyer.png",
		},
		Freelancer: &domain.OrderPreviewUser{
			UserID:      "seller-1",
			Username:    "alex2",
			DisplayName: "Seller",
			AvatarURL:   "seller.png",
		},
	}

	cache := mapDomainToCache(order)
	got := mapCacheToDomain(cache)
	if got.OrderID != order.OrderID || got.GigTitle != order.GigTitle || got.PictureURL != order.PictureURL {
		t.Fatalf("got = %#v", got)
	}
	if got.Customer == nil || got.Freelancer == nil {
		t.Fatalf("participants = %#v %#v", got.Customer, got.Freelancer)
	}
	if got.Customer.Username != "alex1" || got.Freelancer.Username != "alex2" {
		t.Fatalf("participants = %#v %#v", got.Customer, got.Freelancer)
	}
}

func TestMapLegacyCacheToDomain(t *testing.T) {
	got := mapLegacyCacheToDomain(model.LegacyOrderCache{
		OrderID:     "order-1",
		BuyerID:     "buyer-1",
		SellerID:    "seller-1",
		GigID:       "gig-1",
		GigTitle:    "Gig",
		PackageID:   "pkg-1",
		PackageTier: "basic",
		PriceCents:  1000,
		Currency:    "usd",
		Status:      domain.OrderStatusFunded,
		CreatedAt:   time.Unix(100, 0).UTC().Format(time.RFC3339Nano),
		UpdatedAt:   time.Unix(200, 0).UTC().Format(time.RFC3339Nano),
	})
	if got.OrderID != "order-1" || got.Status != domain.OrderStatusFunded {
		t.Fatalf("got = %#v", got)
	}
}
