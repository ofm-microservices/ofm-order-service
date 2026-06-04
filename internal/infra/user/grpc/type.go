package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	userv1 "github.com/ofm-microservices/ofm-common/proto/user/v1"
	app "order-service/internal/application"
)

// Logger aliases the shared logger contract used by the user gRPC adapter.
type Logger = logging.Logger

// UserService exposes the upstream user-service client used to hydrate order
// page participant snapshots.
type UserService interface {
	GetUserPreviewByIDNoCache(ctx context.Context, userID string) (*app.UserPreview, error)
	Close() error
}

// UserMapper translates between order-service inputs and the shared user gRPC
// contract.
type UserMapper interface {
	ToGetUserPreviewByIDNoCacheRequest(userID string) *userv1.GetUserPreviewByIDNoCacheRequest
	ToGetUserPreviewByIDNoCacheResponse(res *userv1.GetUserPreviewByIDNoCacheResponse) *app.UserPreview
	ToError(err error) error
}
