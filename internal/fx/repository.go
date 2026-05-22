package appfx

import (
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"order-service/internal/domain"
	readrepo "order-service/internal/infra/read/redis"
	writerepo "order-service/internal/infra/write/yugabyte"
)

// RepoModule wires write- and read-model repositories into the FX graph.
var RepoModule = fx.Options(fx.Provide(ProvideWriteRepo, ProvideReadRepo))

// ProvideWriteRepo constructs the Yugabyte-backed order repository.
func ProvideWriteRepo(dbx *sqlx.DB, lg logging.Logger) (domain.OrderRepository, error) {
	return writerepo.New(dbx, lg)
}

// ProvideReadRepo constructs the Redis-backed order read repository.
func ProvideReadRepo(rdb *redis.Client, lg logging.Logger) (domain.OrderReadRepository, error) {
	return readrepo.New(rdb, lg)
}
