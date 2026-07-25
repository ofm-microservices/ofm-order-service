package redis

import (
	"strings"
	"time"

	"order-service/internal/domain"
	"order-service/internal/infra/read/redis/model"
)

func mapDeliveryToCache(delivery *domain.OrderDeliveryProjection) model.DeliveryCache {
	if delivery == nil {
		return model.DeliveryCache{}
	}
	out := model.DeliveryCache{
		OrderDeliveryFiles: make([]model.DeliveryFileSnapshot, 0, len(delivery.DeliveryFiles)),
	}
	if delivery.Delivery != nil {
		out.OrderDelivery = model.OrderDeliverySnapshot{
			DeliveryMessage: strings.TrimSpace(delivery.Delivery.DeliveryMessage),
			CreatedAt:       delivery.Delivery.CreatedAt.UTC().Format(time.RFC3339Nano),
		}
	}
	for _, file := range delivery.DeliveryFiles {
		out.OrderDeliveryFiles = append(out.OrderDeliveryFiles, model.DeliveryFileSnapshot{
			FileID:    strings.TrimSpace(file.FileID),
			FileURL:   strings.TrimSpace(file.FileURL),
			SortOrder: file.SortOrder,
			CreatedAt: file.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return out
}

func mapDeliveryCacheToDomain(orderID string, cache model.DeliveryCache) *domain.OrderDeliveryProjection {
	out := &domain.OrderDeliveryProjection{
		OrderID: strings.TrimSpace(orderID),
	}
	if strings.TrimSpace(cache.OrderDelivery.DeliveryMessage) != "" || strings.TrimSpace(cache.OrderDelivery.CreatedAt) != "" {
		out.Delivery = &domain.OrderDelivery{
			OrderID:         strings.TrimSpace(orderID),
			DeliveryMessage: strings.TrimSpace(cache.OrderDelivery.DeliveryMessage),
			CreatedAt:       parseTimeOrZero(cache.OrderDelivery.CreatedAt),
		}
	}
	out.DeliveryFiles = make([]domain.OrderDeliveryFile, 0, len(cache.OrderDeliveryFiles))
	for _, file := range cache.OrderDeliveryFiles {
		out.DeliveryFiles = append(out.DeliveryFiles, domain.OrderDeliveryFile{
			OrderID:   strings.TrimSpace(orderID),
			FileID:    strings.TrimSpace(file.FileID),
			FileURL:   strings.TrimSpace(file.FileURL),
			SortOrder: file.SortOrder,
			CreatedAt: parseTimeOrZero(file.CreatedAt),
		})
	}
	return out
}
