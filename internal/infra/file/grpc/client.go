package grpc

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	filev1 "github.com/ofm-microservices/ofm-common/proto/file/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"order-service/config"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   filev1.FileServiceClient
	log  logging.Logger
	mapr *fileMapper
}

// New constructs the gRPC client used by order-service to resolve public file URLs.
func New(cfg config.FileServiceConfig, log logging.Logger) (FileService, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, ErrEmptyFileServiceAddress
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
		cl:   filev1.NewFileServiceClient(conn),
		log:  log.With(logging.String("module", "grpc-file-client"), logging.String("address", cfg.Address)),
		mapr: newFileMapper(log),
	}, nil
}

func (c *client) GetFileURL(ctx context.Context, fileID string) (string, error) {
	res, err := c.cl.GetFileURL(ctx, c.mapr.ToGetFileURLRequest(fileID))
	if err != nil {
		return "", c.mapr.ToError(err)
	}
	return c.mapr.ToGetFileURLResponse(res), nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.log.Info("closing file service grpc client")
	return c.conn.Close()
}
