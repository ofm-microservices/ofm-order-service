package grpc

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	userv1 "github.com/ofm-microservices/ofm-common/proto/user/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"order-service/config"
	app "order-service/internal/application"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   userv1.UserQueryServiceClient
	mapr UserMapper
	log  logging.Logger
}

// New constructs the gRPC client used by order-service to resolve public user snapshots.
func New(cfg config.UserServiceConfig, log logging.Logger) (UserService, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, ErrEmptyUserServiceAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	conn, err := grpcpkg.NewClient(
		cfg.Address,
		grpcpkg.WithTransportCredentials(insecure.NewCredentials()),
		grpcpkg.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpcpkg.WithUnaryInterceptor(metrics.UnaryClientInterceptor()),
	)
	if err != nil {
		return nil, err
	}
	return &client{
		conn: conn,
		cl:   userv1.NewUserQueryServiceClient(conn),
		mapr: newUserMapper(log),
		log:  log.With(logging.String("module", "grpc-user-client"), logging.String("address", cfg.Address)),
	}, nil
}

func (c *client) GetUserPreviewByIDNoCache(ctx context.Context, userID string) (*app.UserPreview, error) {
	res, err := c.cl.GetUserPreviewByIDNoCache(ctx, c.mapr.ToGetUserPreviewByIDNoCacheRequest(strings.TrimSpace(userID)))
	if err != nil {
		return nil, c.mapr.ToError(err)
	}
	return c.mapr.ToGetUserPreviewByIDNoCacheResponse(res), nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing user service grpc client")
	return c.conn.Close()
}
