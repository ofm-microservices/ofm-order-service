package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// Logger aliases the shared logger contract used by the file gRPC adapter.
type Logger = logging.Logger

// FileService exposes the upstream file-service client used to resolve order
// gig picture URLs.
type FileService interface {
	GetFileURL(ctx context.Context, fileID string) (string, error)
	Close() error
}
