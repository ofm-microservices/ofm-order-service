package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	cache := mapDomainToCache(order)
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

func (r *repo) GetByID(ctx context.Context, orderID string) (*domain.Order, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("get", "order", status, time.Since(started)) }()

	raw, err := r.rdb.Get(ctx, OrderKey(orderID)).Bytes()
	if err != nil {
		status = "error"
		if err == redis.Nil {
			return nil, domain.ErrOrderNotFound
		}
		r.log.Error("get order cache failed", logging.Operation("redis.order.get"), logging.DurationMS(time.Since(started)), logging.String("order_id", orderID), logging.Err(err))
		return nil, err
	}
	var cache model.OrderCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		var legacy model.LegacyOrderCache
		if legacyErr := json.Unmarshal(raw, &legacy); legacyErr != nil {
			status = "error"
			r.log.Error("unmarshal order cache failed", logging.Operation("redis.order.get"), logging.DurationMS(time.Since(started)), logging.String("order_id", orderID), logging.Err(err))
			return nil, err
		}
		return mapLegacyCacheToDomain(legacy), nil
	}
	if strings.TrimSpace(cache.Order.OrderID) == "" {
		var legacy model.LegacyOrderCache
		if legacyErr := json.Unmarshal(raw, &legacy); legacyErr == nil && strings.TrimSpace(legacy.OrderID) != "" {
			return mapLegacyCacheToDomain(legacy), nil
		}
		return nil, domain.ErrOrderNotFound
	}
	return mapCacheToDomain(cache), nil
}

func (r *repo) GetPreviewByID(ctx context.Context, orderID string) (*domain.OrderPreview, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("get", "order_preview", status, time.Since(started)) }()

	raw, err := r.rdb.Get(ctx, OrderKey(orderID)).Bytes()
	if err != nil {
		status = "error"
		if err == redis.Nil {
			return nil, domain.ErrOrderNotFound
		}
		r.log.Error("get order preview cache failed", logging.Operation("redis.order_preview.get"), logging.DurationMS(time.Since(started)), logging.String("order_id", orderID), logging.Err(err))
		return nil, err
	}
	var cache model.OrderCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		var legacy model.LegacyOrderCache
		if legacyErr := json.Unmarshal(raw, &legacy); legacyErr != nil {
			status = "error"
			r.log.Error("unmarshal order preview cache failed", logging.Operation("redis.order_preview.get"), logging.DurationMS(time.Since(started)), logging.String("order_id", orderID), logging.Err(err))
			return nil, err
		}
		return &domain.OrderPreview{
			OrderID:   legacy.OrderID,
			CreatedAt: parseTimeOrZero(legacy.CreatedAt),
			Status:    legacy.Status,
		}, nil
	}
	if strings.TrimSpace(cache.Order.OrderID) == "" {
		var legacy model.LegacyOrderCache
		if legacyErr := json.Unmarshal(raw, &legacy); legacyErr == nil && strings.TrimSpace(legacy.OrderID) != "" {
			return &domain.OrderPreview{
				OrderID:   legacy.OrderID,
				CreatedAt: parseTimeOrZero(legacy.CreatedAt),
				Status:    legacy.Status,
			}, nil
		}
		return nil, domain.ErrOrderNotFound
	}
	return &domain.OrderPreview{
		OrderID:   cache.Order.OrderID,
		CreatedAt: parseTimeOrZero(cache.Order.CreatedAt),
		Status:    cache.Order.Status,
	}, nil
}
