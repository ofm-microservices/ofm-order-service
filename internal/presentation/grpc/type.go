package grpc

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	app "order-service/internal/application"
)

type Logger = logging.Logger
type Service = app.Service
type Server interface {
	Start() error
	Shutdown(context.Context) error
}
