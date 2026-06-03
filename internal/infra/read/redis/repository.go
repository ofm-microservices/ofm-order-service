package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/redis/go-redis/v9"
	"order-service/internal/domain"
	"order-service/internal/infra/read/redis/model"
)

type repo struct {
	rdb *redis.Client
	log logging.Logger
}

// New constructs the Redis-backed order read repository.
func New(rdb *redis.Client, log logging.Logger) (domain.OrderReadRepository, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &repo{rdb: rdb, log: log.With(logging.String("module", "redis-repository"))}, nil
}

// OrderKey builds the Redis key used for order projections.
func OrderKey(orderID string) string { return fmt.Sprintf("order:%s", orderID) }

func (r *repo) Upsert(ctx context.Context, order *domain.Order) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("set", "order", status, time.Since(started)) }()
	if order == nil {
		return ErrNilOrder
	}
	cache := model.OrderCache{
		OrderID:             order.OrderID,
		SagaID:              order.SagaID,
		BuyerID:             order.BuyerID,
		SellerID:            order.SellerID,
		SellerUsername:      order.SellerUsername,
		GigID:               order.GigID,
		GigTitle:            order.GigTitle,
		PackageID:           order.PackageID,
		PackageTier:         order.PackageTier,
		PackageDescription:  order.PackageDescription,
		PackageDeliveryDays: order.PackageDeliveryDays,
		PriceCents:          order.PriceCents,
		Currency:            order.Currency,
		Status:              order.Status,
		PaymentIntentID:     order.PaymentIntentID,
		CreatedAt:           order.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:           order.UpdatedAt.Format(time.RFC3339Nano),
	}
	payload, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, OrderKey(order.OrderID), payload, 0).Err(); err != nil {
		status = "error"
		r.log.Error("upsert order cache failed", logging.Operation("redis.order.upsert"), logging.DurationMS(time.Since(started)), logging.Err(err))
		return err
	}
	return nil
}
