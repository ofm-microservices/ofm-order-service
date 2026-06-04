package redis

import (
	"time"

	"order-service/internal/domain"
	"order-service/internal/infra/read/redis/model"
)

func mapDomainToCache(order *domain.Order) model.OrderCache {
	cache := model.OrderCache{
		Order: model.OrderSnapshot{
			OrderID:   order.OrderID,
			CreatedAt: order.CreatedAt.Format(time.RFC3339Nano),
			Status:    order.Status,
			UpdatedAt: order.UpdatedAt.Format(time.RFC3339Nano),
		},
		Gig: model.GigSnapshot{
			GigID:      order.GigID,
			Title:      order.GigTitle,
			PictureURL: order.PictureURL,
			Package: model.PackageSnapshot{
				PackageID:           order.PackageID,
				Title:               order.PackageTier,
				PriceCents:          order.PriceCents,
				Currency:            order.Currency,
				Description:         order.PackageDescription,
				PackageDeliveryDays: order.PackageDeliveryDays,
			},
		},
		BuyerID:         order.BuyerID,
		SellerID:        order.SellerID,
		SellerUsername:  order.SellerUsername,
		PictureFileID:   order.PictureFileID,
		PaymentIntentID: order.PaymentIntentID,
	}
	if order.Customer != nil {
		cache.Customer = &model.UserSnapshot{
			UserID:      order.Customer.UserID,
			Username:    order.Customer.Username,
			DisplayName: order.Customer.DisplayName,
			AvatarURL:   order.Customer.AvatarURL,
		}
	}
	if order.Freelancer != nil {
		cache.Freelancer = &model.UserSnapshot{
			UserID:      order.Freelancer.UserID,
			Username:    order.Freelancer.Username,
			DisplayName: order.Freelancer.DisplayName,
			AvatarURL:   order.Freelancer.AvatarURL,
		}
	}
	return cache
}

func mapCacheToDomain(cache model.OrderCache) *domain.Order {
	order := &domain.Order{
		OrderID:             cache.Order.OrderID,
		BuyerID:             cache.BuyerID,
		SellerID:            cache.SellerID,
		SellerUsername:      cache.SellerUsername,
		GigID:               cache.Gig.GigID,
		GigTitle:            cache.Gig.Title,
		PictureFileID:       cache.PictureFileID,
		PictureURL:          cache.Gig.PictureURL,
		PackageID:           cache.Gig.Package.PackageID,
		PackageTier:         cache.Gig.Package.Title,
		PackageDescription:  cache.Gig.Package.Description,
		PackageDeliveryDays: cache.Gig.Package.PackageDeliveryDays,
		PriceCents:          cache.Gig.Package.PriceCents,
		Currency:            cache.Gig.Package.Currency,
		Status:              cache.Order.Status,
		PaymentIntentID:     cache.PaymentIntentID,
		CreatedAt:           parseTimeOrZero(cache.Order.CreatedAt),
		UpdatedAt:           parseTimeOrZero(cache.Order.UpdatedAt),
	}
	if cache.Customer != nil {
		order.Customer = &domain.OrderPreviewUser{
			UserID:      cache.Customer.UserID,
			Username:    cache.Customer.Username,
			DisplayName: cache.Customer.DisplayName,
			AvatarURL:   cache.Customer.AvatarURL,
		}
	}
	if cache.Freelancer != nil {
		order.Freelancer = &domain.OrderPreviewUser{
			UserID:      cache.Freelancer.UserID,
			Username:    cache.Freelancer.Username,
			DisplayName: cache.Freelancer.DisplayName,
			AvatarURL:   cache.Freelancer.AvatarURL,
		}
	}
	return order
}

func mapLegacyCacheToDomain(cache model.LegacyOrderCache) *domain.Order {
	return &domain.Order{
		OrderID:             cache.OrderID,
		SagaID:              cache.SagaID,
		BuyerID:             cache.BuyerID,
		SellerID:            cache.SellerID,
		SellerUsername:      cache.SellerUsername,
		GigID:               cache.GigID,
		GigTitle:            cache.GigTitle,
		PictureFileID:       cache.PictureFileID,
		PictureURL:          cache.PictureURL,
		PackageID:           cache.PackageID,
		PackageTier:         cache.PackageTier,
		PackageDescription:  cache.PackageDescription,
		PackageDeliveryDays: cache.PackageDeliveryDays,
		PriceCents:          cache.PriceCents,
		Currency:            cache.Currency,
		Status:              cache.Status,
		PaymentIntentID:     cache.PaymentIntentID,
		CreatedAt:           parseTimeOrZero(cache.CreatedAt),
		UpdatedAt:           parseTimeOrZero(cache.UpdatedAt),
	}
}

func parseTimeOrZero(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}
