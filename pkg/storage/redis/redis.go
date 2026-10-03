package redis

import (
	"context"
	"fmt"

	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"order-service/config"
)

// Open creates the Redis client used for the order read model and verifies the
// connection with a ping.
func Open(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	rdb.AddHook(&breakerHook{breaker: resilience.NewBreaker(resilience.BreakerConfigFromEnv())})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return nil, err
	}
	if err := resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, _ int) error { return rdb.Ping(attemptCtx).Err() }); err != nil {
		return nil, WrapRedisPingError(err)
	}
	return rdb, nil
}

type breakerHook struct{ breaker *resilience.Breaker }

func (h *breakerHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *breakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return h.breaker.Do(ctx, func(callCtx context.Context) error { return next(callCtx, cmd) })
	}
}
func (h *breakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return h.breaker.Do(ctx, func(callCtx context.Context) error { return next(callCtx, cmds) })
	}
}
