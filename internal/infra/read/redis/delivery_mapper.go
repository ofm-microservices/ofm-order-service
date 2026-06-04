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
