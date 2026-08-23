package yugabyte

import (
	"fmt"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/attribute"
	"order-service/config"
)

var connectDB = sqlx.Connect

// Open creates the order-service YugabyteDB connection pool.
func Open(cfg config.DBConfig) (*sqlx.DB, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, cfg.SSLMode)
	driverName, err := otelsql.Register("pgx", otelsql.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.namespace", cfg.Name)))
	if err != nil {
		return nil, WrapOpenDBError(err)
	}
	dbx, err := connectDB(driverName, dsn)
	if err != nil {
		return nil, WrapOpenDBError(err)
	}
	dbx.SetMaxOpenConns(cfg.MaxOpenConns)
	dbx.SetMaxIdleConns(cfg.MaxIdleConns)
	dbx.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	dbx.SetConnMaxIdleTime(2 * time.Minute)
	return dbx, nil
}
