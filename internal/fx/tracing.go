package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	sharedtracing "github.com/ofm-microservices/ofm-common/pkg/observability/tracing"
	"go.uber.org/fx"
	"order-service/config"
)

// TracingModule wires OpenTelemetry tracing for order-service.
var TracingModule = fx.Options(
	fx.Provide(ProvideTracingConfig),
	fx.Invoke(InvokeInstallTracing),
)

// ProvideTracingConfig maps service config into the shared tracing config.
func ProvideTracingConfig(cfg *config.Config) *sharedtracing.Config {
	tcfg := sharedtracing.DefaultConfig("order-service", cfg.App.Env)
	tcfg.Enabled = cfg.Tracing.Enabled
	tcfg.Endpoint = cfg.Tracing.Endpoint
	tcfg.Protocol = cfg.Tracing.Protocol
	tcfg.SampleRatio = cfg.Tracing.SampleRatio
	tcfg.ServiceVersion = cfg.Tracing.ServiceVersion
	return &tcfg
}

// InvokeInstallTracing installs the process-wide tracer provider.
func InvokeInstallTracing(lc fx.Lifecycle, cfg *sharedtracing.Config, lg logging.Logger) {
	if cfg == nil {
		return
	}
	tp, err := sharedtracing.NewTracerProvider(context.Background(), *cfg)
	if err != nil {
		lg.Error("tracing initialization failed", logging.Err(err))
		return
	}
	sharedtracing.SetGlobal(tp)
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return tp.Shutdown(context.Background()) }})
	lg.Info("tracing initialized")
}
