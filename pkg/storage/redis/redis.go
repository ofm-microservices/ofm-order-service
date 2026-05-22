package redis

import (
	"context"
	"fmt"

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
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, WrapRedisPingError(err)
	}
	return rdb, nil
}
